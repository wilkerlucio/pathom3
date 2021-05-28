(ns com.wsscode.pathom3.interface.eql-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]))

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
      (is (= (p.eql/process (-> (pci/register geo/full-registry)
                                (p.ent/with-entity {:left 10}))
                            [::geo/top])
             {}))))

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
    (is (= (p.eql/process (-> (pci/register registry)
                              (p.ent/with-entity {::coords (list
                                                             {:x 10 :y 20}
                                                             {::geo/left 20 ::geo/width 5})}))
                          [{::coords [:right]}])
           {::coords [{} {:right 25}]})))

  (testing "process vector"
    (let [res (p.eql/process (-> (pci/register registry)
                                 (p.ent/with-entity {::coords [{:x 10 :y 20}
                                                               {::geo/left 20 ::geo/width 5}]}))
                             [{::coords [:right]}])]
      (is (= res {::coords [{} {:right 25}]}))
      (is (vector? (::coords res)))))

  (testing "process set"
    (is (= (p.eql/process (-> (pci/register registry)
                              (p.ent/with-entity {::coords #{{:x 10 :y 20}
                                                             {::geo/left 20 ::geo/width 5}}}))
                          [{::coords [:right]}])
           {::coords #{{} {:right 25}}}))))

(deftest boundary-interface-test
  (let [fi (p.eql/boundary-interface (pci/register registry))]
    (testing "call with just tx"
      (is (= (fi [::coords])
             {::coords
              [{:x 10 :y 20}
               {::geo/left 20 ::geo/width 5}]})))

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
             {:new "value"})))))
