(ns com.wsscode.pathom3.interface.eql-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.interface.eql :as peql]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]))

(pco/defresolver coords [] ::coords
  [{:x 10 :y 20}
   {::geo/left 20 ::geo/width 5}])

(def registry
  [geo/full-registry
   coords])

(deftest process-test
  (testing "simple read"
    (is (= (peql/process (-> (pci/register geo/full-registry)
                             (p.ent/with-entity {:left 10}))
                         [::geo/x])
           {::geo/x 10})))

  (testing "nested read"
    (is (= (peql/process (-> (pci/register geo/full-registry)
                             (p.ent/with-entity {:left 10 :top 5}))
                         [{::geo/turn-point [:right]}])
           {::geo/turn-point {:right 10}}))

    (is (= (peql/process (-> (pci/register geo/full-registry)
                             (p.ent/with-entity {:foo            {::geo/x 10}
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
    (is (= (peql/process (-> (pci/register registry)
                             (p.ent/with-entity {::coords (list
                                                            {:x 10 :y 20}
                                                            {::geo/left 20 ::geo/width 5})}))
                         [{::coords [:right]}])
           {::coords [{} {:right 25}]})))

  (testing "process vector"
    (let [res (peql/process (-> (pci/register registry)
                                (p.ent/with-entity {::coords [{:x 10 :y 20}
                                                              {::geo/left 20 ::geo/width 5}]}))
                            [{::coords [:right]}])]
      (is (= res {::coords [{} {:right 25}]}))
      (is (vector? (::coords res)))))

  (testing "process set"
    (is (= (peql/process (-> (pci/register registry)
                             (p.ent/with-entity {::coords #{{:x 10 :y 20}
                                                            {::geo/left 20 ::geo/width 5}}}))
                         [{::coords [:right]}])
           {::coords #{{} {:right 25}}}))))
