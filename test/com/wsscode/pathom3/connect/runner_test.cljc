(ns com.wsscode.pathom3.connect.runner-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]
    [edn-query-language.core :as eql]))

(deftest all-requires-ready?-test
  (is (= (pcr/all-requires-ready? (p.ent/with-cache-tree {} {:a 1})
           {::pcp/requires {}})
         true))

  (is (= (pcr/all-requires-ready? (p.ent/with-cache-tree {} {:a 1})
           {::pcp/requires {:a {}}})
         true))

  (is (= (pcr/all-requires-ready? (p.ent/with-cache-tree {} {:a 1})
           {::pcp/requires {:b {}}})
         false)))

(deftest merge-resolver-response!-test
  (testing "does nothing when response is not a map"
    (is (= (-> (pcr/merge-resolver-response!
                 (p.ent/with-cache-tree {} {:foo "bar"})
                 nil)
               ::p.ent/cache-tree* deref)
           {:foo "bar"})))

  (testing "adds new data to cache tree"
    (is (= (-> (pcr/merge-resolver-response!
                 (p.ent/with-cache-tree {} {:foo "bar"})
                 {:buz "baz"})
               ::p.ent/cache-tree* deref)
           {:foo "bar"
            :buz "baz"}))))

(deftest run-node!-test
  (is (= (let [tree  {::geo/left 10 ::geo/width 30}
               env   (p.ent/with-cache-tree (pci/register {} geo/registry)
                                            tree)
               graph (pcp/compute-run-graph
                       (-> env
                           (assoc
                             ::pcp/available-data (pfsd/data->shape-descriptor tree)
                             :edn-query-language.ast/node (eql/query->ast [::geo/right
                                                                           ::geo/center-x]))))
               env   (assoc env ::pcp/graph graph)]
           (pcr/run-node! env (pcp/get-root-node graph))
           @(::p.ent/cache-tree* env))
         {::geo/left       10
          ::geo/width      30
          ::geo/right      40
          ::geo/half-width 15
          ::geo/center-x   25})))

(deftest run-graph!-test
  (is (= (let [tree  {::geo/left 10 ::geo/width 30}
               env   (p.ent/with-cache-tree (pci/register {} geo/registry)
                                            tree)
               graph (pcp/compute-run-graph
                       (-> env
                           (assoc
                             ::pcp/available-data (pfsd/data->shape-descriptor tree)
                             :edn-query-language.ast/node (eql/query->ast [::geo/right
                                                                           ::geo/center-x]))))]
           (pcr/run-graph! (assoc env ::pcp/graph graph))
           @(::p.ent/cache-tree* env))
         {::geo/left       10
          ::geo/width      30
          ::geo/right      40
          ::geo/half-width 15
          ::geo/center-x   25})))
