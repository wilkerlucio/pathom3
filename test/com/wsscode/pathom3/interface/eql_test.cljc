(ns com.wsscode.pathom3.interface.eql-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]
    [edn-query-language.core :as eql]))

(pco/defresolver coords []
  {::coords
   [{:x 10 :y 20}
    {::geo/left 20 ::geo/width 5}]})

(def registry
  [geo/full-registry
   coords
   (pbir/constantly-fn-resolver :foo ::foo)])

(deftest process-test
  (testing "read"
    (is (= (p.eql/process (pci/register registry)
                          [::coords])
           {::coords
            [{:x 10 :y 20}
             {::geo/left 20 ::geo/width 5}]}))

    (is (= (p.eql/process (pci/register geo/full-registry)
                          {:left 10}
                          [::geo/x])
           {::geo/x 10}))

    (testing "when not found, key is omitted"
      (is (= (p.eql/process (-> (pci/register
                                  {:com.wsscode.pathom3.error/lenient-mode? true}
                                  geo/full-registry)
                                (p.ent/with-entity {:left 10}))
                            [::geo/top])
             {:com.wsscode.pathom3.connect.runner/attribute-errors {:com.wsscode.pathom3.test.geometry-resolvers/top {:com.wsscode.pathom3.error/cause :com.wsscode.pathom3.error/attribute-unreachable}}}))))

  (testing "reading with *"
    (is (= (-> (p.eql/process (-> (pci/register geo/full-registry)
                                  (p.ent/with-entity {:left 10}))
                              [::geo/x '*]))
           {::geo/x    10
            ::geo/left 10
            :left      10})))

  (testing "nested read"
    (is (= (p.eql/process (-> (pci/register geo/full-registry)
                              (p.ent/with-entity {:left 10 :top 5}))
                          [{::geo/turn-point [:right]}])
           {::geo/turn-point {:right 10}}))

    (is (= (p.eql/process (-> (pci/register geo/full-registry)
                              (p.ent/with-entity {:foo        {::geo/x 10}
                                                  :bar        {::geo/y 4
                                                               :mess   "here"}
                                                  ::geo/width 50
                                                  :other      "value"}))
                          [{:foo [:x]}
                           {:bar [:top]}
                           :width])
           {:foo   {:x 10}
            :bar   {:top 4}
            :width 50})))

  (testing "process sequence"
    (is (= (p.eql/process (-> {:com.wsscode.pathom3.error/lenient-mode? true}
                              (pci/register registry)
                              (p.ent/with-entity {::coords (list
                                                             {:x 10 :y 20}
                                                             {::geo/left 20 ::geo/width 5})}))
                          [{::coords [:right]}])
           {::coords [{:com.wsscode.pathom3.connect.runner/attribute-errors {:right {:com.wsscode.pathom3.error/cause :com.wsscode.pathom3.error/attribute-unreachable}}}
                      {:right 25}]})))

  (testing "process vector"
    (let [res (p.eql/process (-> (pci/register registry)
                                 (p.ent/with-entity {::coords [{::geo/left 20 ::geo/width 15}
                                                               {::geo/left 20 ::geo/width 5}]}))
                             [{::coords [:right]}])]
      (is (= res {::coords [{:right 35} {:right 25}]}))
      (is (vector? (::coords res)))))

  (testing "process set"
    (is (= (p.eql/process (-> (pci/register registry)
                              (p.ent/with-entity {::coords #{{::geo/left 20 ::geo/width 15}
                                                             {::geo/left 20 ::geo/width 5}}}))
                          [{::coords [:right]}])
           {::coords #{{:right 35} {:right 25}}}))))

(deftest process-one-test
  (is (= (p.eql/process-one (pci/register registry) {:left 10 :right 30} :width)
         20))

  (is (= (p.eql/process-one (pci/register geo/full-registry)
                            {:left 10 :top 5}
                            {::geo/turn-point [:right]})
         {:right 10})))

(deftest boundary-interface-test
  (let [fi (p.eql/boundary-interface (pci/register registry))]
    (testing "call with just tx"
      (is (= (fi [::coords])
             {::coords
              [{:x 10 :y 20}
               {::geo/left 20 ::geo/width 5}]})))

    (testing "call with ast"
      (is (= (fi {:pathom/ast (eql/query->ast [::coords])})
             {::coords
              [{:x 10 :y 20}
               {::geo/left 20 ::geo/width 5}]})))

    (testing "call with entity and eql"
      (is (= (fi {:pathom/entity {:left 10}
                  :pathom/eql    [:x]})
             {:x 10})))

    (testing "call with entity and tx"
      (is (= (fi {:pathom/entity {:left 10}
                  :pathom/tx     [:x]})
             {:x 10})))

    (testing "merge env"
      (is (= (fi [:foo])
             {:foo nil}))

      (is (= (fi {::foo "bar"} [:foo])
             {:foo "bar"})))

    (testing "modify env"
      (is (= (fi #(pci/register % (pbir/constantly-resolver :new "value")) [:new])
             {:new "value"})))

    (testing "lenient mode"
      (is (= (fi {:pathom/eql           [:invalid]
                  :pathom/lenient-mode? true})
             {:com.wsscode.pathom3.connect.runner/attribute-errors {:invalid {:com.wsscode.pathom3.error/cause :com.wsscode.pathom3.error/attribute-unreachable}}})))))
