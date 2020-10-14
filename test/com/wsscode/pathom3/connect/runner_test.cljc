(ns com.wsscode.pathom3.connect.runner-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.path :as p.path]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]
    [edn-query-language.core :as eql]))

(deftest all-requires-ready?-test
  (is (= (pcr/all-requires-ready? (p.ent/with-entity {} {:a 1})
           {::pcp/requires {}})
         true))

  (is (= (pcr/all-requires-ready? (p.ent/with-entity {} {:a 1})
           {::pcp/requires {:a {}}})
         true))

  (is (= (pcr/all-requires-ready? (p.ent/with-entity {} {:a 1})
           {::pcp/requires {:b {}}})
         false)))

(deftest merge-resolver-response!-test
  (testing "does nothing when response is not a map"
    (is (= (-> (pcr/merge-resolver-response!
                 (p.ent/with-entity {} {:foo "bar"})
                 nil)
               ::p.ent/entity-tree* deref)
           {:foo "bar"})))

  (testing "adds new data to cache tree"
    (is (= (-> (pcr/merge-resolver-response!
                 (p.ent/with-entity {::p.path/path []
                                     ::pcp/graph   {::pcp/nodes     {}
                                                    ::pcp/index-ast {}}} {:foo "bar"})
                 {:buz "baz"})
               ::p.ent/entity-tree* deref)
           {:foo "bar"
            :buz "baz"}))))

(deftest run-node!-test
  (is (= (let [tree  {::geo/left 10 ::geo/width 30}
               env   (p.ent/with-entity (pci/register {::p.path/path []} geo/registry)
                                        tree)
               graph (pcp/compute-run-graph
                       (-> env
                           (assoc
                             ::pcp/available-data (pfsd/data->shape-descriptor tree)
                             :edn-query-language.ast/node (eql/query->ast [::geo/right
                                                                           ::geo/center-x]))))
               env   (assoc env ::pcp/graph graph
                       ::pcr/node-run-stats* (atom {}))]
           (pcr/run-node! env (pcp/get-root-node graph))
           @(::p.ent/entity-tree* env))
         {::geo/left       10
          ::geo/width      30
          ::geo/right      40
          ::geo/half-width 15
          ::geo/center-x   25})))

(defn run-graph [env tree query]
  (let [env   (-> env
                  (p.ent/with-entity tree))
        ast   (eql/query->ast query)
        graph (pcp/compute-run-graph
                (-> env
                    (assoc
                      ::pcp/available-data (pfsd/data->shape-descriptor tree)
                      :edn-query-language.ast/node ast)))]
    (pcr/run-graph!* (assoc env ::pcp/graph graph
                       ::pcr/node-run-stats* (atom {})))
    @(::p.ent/entity-tree* env)))

(defn coords-resolver [c]
  (pco/resolver 'coords-resolver {::pco/output [::coords]}
    (fn [_ _] {::coords c})))

(pco/defresolver current-path [{::p.path/keys [path]} _]
  {::p.path/path path})

(deftest run-graph!-test
  (is (= (run-graph (pci/register geo/registry)
                    {::geo/left 10 ::geo/width 30}
                    [::geo/right ::geo/center-x])
         {::geo/left       10
          ::geo/width      30
          ::geo/right      40
          ::geo/half-width 15
          ::geo/center-x   25}))

  (is (= (run-graph (pci/register [geo/full-registry])
                    {:data {::geo/x 10}}
                    [{:data [:left]}])
         {:data {::geo/x    10
                 ::geo/left 10
                 :left      10}}))

  (testing "ident"
    (is (= (run-graph (pci/register [geo/full-registry])
                      {}
                      [[::geo/x 10]])
           {[::geo/x 10] {::geo/x 10}}))

    (is (= (run-graph (pci/register [geo/full-registry])
                      {}
                      [{[::geo/x 10] [::geo/left]}])
           {[::geo/x 10] {::geo/x    10
                          ::geo/left 10}}))

    (is (= (run-graph (pci/register [geo/full-registry])
                      {[::geo/x 10] {:random "data"}}
                      [{[::geo/x 10] [::geo/left]}])
           {[::geo/x 10] {:random    "data"
                          ::geo/x    10
                          ::geo/left 10}})))

  (testing "path"
    (is (= (run-graph (pci/register [(pbir/constantly-resolver ::hold {})
                                     (pbir/constantly-resolver ::sequence [{} {}])
                                     current-path])
                      {}
                      [::p.path/path
                       {::hold [::p.path/path]}
                       {::sequence [::p.path/path]}])
           {::p.path/path [],
            ::sequence    [{::p.path/path [::sequence]}
                           {::p.path/path [::sequence]}],
            ::hold        {::p.path/path [::hold]}})))

  #_(testing "insufficient data"
      (is (= (run-graph (pci/register [(pco/resolver 'a {::pco/output [:a]
                                                         ::pco/input  [:b]}
                                         (fn [_ _] {:a "a"}))
                                       (pco/resolver 'b {::pco/output [:b]}
                                         (fn [_ _] {}))])
               {}
               [:a])
             {::coords [{::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                        {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}]})))

  (testing "processing sequence of consistent elements"
    (is (= (run-graph (pci/register [geo/full-registry
                                     (coords-resolver
                                       [{::geo/x 7 ::geo/y 9}
                                        {::geo/x 3 ::geo/y 4}])])
                      {}
                      [{::coords [:left]}])
           {::coords [{::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                      {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}]}))

    (testing "data from join"
      (is (= (run-graph (pci/register geo/full-registry)
                        {::coords [{::geo/x 7 ::geo/y 9}
                                   {::geo/x 3 ::geo/y 4}]}
                        [{::coords [:left]}])
             {::coords [{::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                        {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}]})))

    (testing "set data from join"
      (is (= (run-graph (pci/register geo/full-registry)
                        {::coords #{{::geo/x 7 ::geo/y 9}
                                    {::geo/x 3 ::geo/y 4}}}
                        [{::coords [:left]}])
             {::coords #{{::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                         {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}}})))

    (testing "map values"
      (is (= (run-graph (pci/register geo/full-registry)
                        {::coords ^::pcr/map-container? {:a {::geo/x 7 ::geo/y 9}
                                                         :b {::geo/x 3 ::geo/y 4}}}
                        [{::coords [:left]}])
             {::coords {:a {::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                        :b {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}}}))

      (is (= (run-graph (pci/register geo/full-registry)
                        {::coords {:a {::geo/x 7 ::geo/y 9}
                                   :b {::geo/x 3 ::geo/y 4}}}
                        '[{(::coords {::pcr/map-container? true}) [:left]}])
             {::coords {:a {::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                        :b {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}}}))))

  (testing "processing sequence of inconsistent maps"
    (is (= (run-graph (pci/register geo/full-registry)
                      {::coords [{::geo/x 7 ::geo/y 9}
                                 {::geo/left 7 ::geo/y 9}]}
                      [{::coords [:left]}])
           {::coords
            [{::geo/x    7
              ::geo/y    9
              ::geo/left 7
              :left      7}
             {::geo/left 7
              ::geo/y    9
              :left      7}]})))

  (testing "processing sequence partial items being maps"
    (is (= (run-graph (pci/register geo/full-registry)
                      {::coords [{::geo/x 7 ::geo/y 9}
                                 20]}
                      [{::coords [:left]}])
           {::coords [{::geo/x    7
                       ::geo/y    9
                       ::geo/left 7
                       :left      7}
                      20]})))

  #_(testing "errors"
      (let [error (ex-info "Error" {})]
        (is (= (run-graph (pci/register
                            (pco/resolver 'error {::pco/output [:error]}
                              (fn [_ _] (throw error))))
                 {}
                 [:error])
               {:error       ::pcr/resolver-error
                ::pcr/errors {[:error] error}})))))
