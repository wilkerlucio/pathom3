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
                             (p.ent/with-cache-tree {:left 10}))
                         [::geo/x])
           {::geo/x 10})))

  #_ #_
  (testing "nested read"
    (is (= (peql/process (-> (pci/register geo/full-registry)
                             (p.ent/with-cache-tree {:left 10 :top 5}))
             [{::geo/turn-point [:right]}])
           {::geo/turn-point {:right 10}})))

  (testing "process sequence"
    (is (= (peql/process (pci/register registry)
             [{::coords [:right]}])
           {::coords [{} {:right 25}]}))))
