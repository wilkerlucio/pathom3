(ns com.wsscode.pathom3.connect.runner-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.connect.runner.stats :as pcrs]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.interface.smart-map :as psm]
    [com.wsscode.pathom3.path :as p.path]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]
    [edn-query-language.core :as eql]))

(deftest all-requires-ready?-test
  (is (= (pcr/all-requires-ready? (p.ent/with-entity {} {:a 1})
           {::pcp/expects {}})
         true))

  (is (= (pcr/all-requires-ready? (p.ent/with-entity {} {:a 1})
           {::pcp/expects {:a {}}})
         true))

  (is (= (pcr/all-requires-ready? (p.ent/with-entity {} {:a 1})
           {::pcp/expects {:b {}}})
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
            :buz "baz"}))

    (testing "skip unknown values"
      (is (= (-> (pcr/merge-resolver-response!
                   (p.ent/with-entity {::p.path/path []
                                       ::pcp/graph   {::pcp/nodes     {}
                                                      ::pcp/index-ast {}}} {:foo "bar"})
                   {:buz ::pcr/unknown-value})
                 ::p.ent/entity-tree* deref)
             {:foo "bar"})))))

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
  (let [env       (-> env
                      (p.ent/with-entity tree))
        ast       (eql/query->ast query)
        graph     (pcp/compute-run-graph
                    (-> env
                        (assoc
                          ::pcp/available-data (pfsd/data->shape-descriptor tree)
                          :edn-query-language.ast/node ast)))
        run-stats (pcr/run-graph!* (assoc env ::pcp/graph graph
                                     ::pcr/node-run-stats* (atom {})))]
    (with-meta @(::p.ent/entity-tree* env) {::pcr/run-stats run-stats})))

(defn coords-resolver [c]
  (pco/resolver 'coords-resolver {::pco/output [::coords]}
    (fn [_ _] {::coords c})))

(pco/defresolver current-path [{::p.path/keys [path]} _]
  {::p.path/path path})

(def full-env (pci/register [geo/full-registry]))

(deftest run-graph!-test
  (is (= (run-graph (pci/register geo/registry)
                    {::geo/left 10 ::geo/width 30}
                    [::geo/right ::geo/center-x])
         {::geo/left       10
          ::geo/width      30
          ::geo/right      40
          ::geo/half-width 15
          ::geo/center-x   25}))

  (is (= (run-graph full-env
                    {:data {::geo/x 10}}
                    [{:data [:left]}])
         {:data {::geo/x    10
                 ::geo/left 10
                 :left      10}}))

  (testing "ident"
    (is (= (run-graph full-env
                      {}
                      [[::geo/x 10]])
           {[::geo/x 10] {::geo/x 10}}))

    (is (= (run-graph full-env
                      {}
                      [{[::geo/x 10] [::geo/left]}])
           {[::geo/x 10] {::geo/x    10
                          ::geo/left 10}}))

    (is (= (run-graph full-env
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

  (testing "insufficient data"
    (let [res (run-graph (pci/register [(pco/resolver 'a {::pco/output [:a]
                                                          ::pco/input  [:b]}
                                          (fn [_ _] {:a "a"}))
                                        (pco/resolver 'b {::pco/output [:b]}
                                          (fn [_ _] {}))])
                         {}
                         [:a])]
      (is (= res {}))
      (is (= (-> res meta ::pcr/run-stats
                 ::pcr/node-run-stats
                 (get 1)
                 ::pcr/node-error
                 ex-message)
             "Insufficient data"))
      (is (= (-> res meta ::pcr/run-stats
                 ::pcr/node-run-stats
                 (get 1)
                 ::pcr/node-error
                 ex-data)
             {:available nil
              :required  [:b]}))))

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

  (testing "processing OR nodes"
    (testing "return the first option that works, don't call the others"
      (let [spy (atom 0)]
        (is (= (run-graph (pci/register [(pco/resolver `value
                                           {::pco/output [:error]}
                                           (fn [_ _]
                                             (swap! spy inc)
                                             {:error 1}))
                                         (pco/resolver `value2
                                           {::pco/output [:error]}
                                           (fn [_ _]
                                             (swap! spy inc)
                                             {:error 1}))])
                          {}
                          [:error])
               {:error 1}))
        (is (= @spy 1))))

    (testing "one option fail, one succeed"
      (let [spy (atom 0)]
        (is (= (run-graph (pci/register [(pco/resolver `error-long-touch
                                           {::pco/output [:error]}
                                           (fn [_ _]
                                             (swap! spy inc)
                                             (throw (ex-info "Error" {}))))
                                         (pbir/constantly-resolver :error "value")])
                          {}
                          [:error])
               {:error "value"}))
        (is (= @spy 1)))))

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

  (testing "placeholders"
    (is (= (run-graph (pci/register (pbir/constantly-resolver :foo "bar"))
                      {}
                      [{:>/path [:foo]}])
           {:foo    "bar"
            :>/path {:foo "bar"}}))

    (is (= (run-graph (pci/register (pbir/constantly-resolver :foo "bar"))
                      {:foo "baz"}
                      [{:>/path [:foo]}])
           {:foo    "baz"
            :>/path {:foo "baz"}}))

    (testing "modified data"
      (is (= (run-graph (pci/register
                          [(pbir/single-attr-resolver :x :y #(* 2 %))])
                        {}
                        '[{(:>/path {:x 20}) [:y]}])
             {:>/path {:x 20
                       :y 40}}))

      (is (= (run-graph (pci/register
                          [(pbir/constantly-resolver :x 10)
                           (pbir/single-attr-resolver :x :y #(* 2 %))])
                        {}
                        '[{(:>/path {:x 20}) [:y]}])
             {:x      10
              :y      20
              :>/path {:x 20
                       :y 40}}))

      (is (= (run-graph (pci/register
                          [(pbir/constantly-resolver :x 10)
                           (pbir/single-attr-resolver :x :y #(* 2 %))])
                        {}
                        '[:x
                          {(:>/path {:x 20}) [:y]}])
             {:x      10
              :y      20
              :>/path {:x 20
                       :y 40}}))))

  (testing "errors"
    (let [error (ex-info "Error" {})
          stats (-> (run-graph (pci/register
                                 (pco/resolver 'error {::pco/output [:error]}
                                   (fn [_ _] (throw error))))
                               {}
                               [:error])
                    meta ::pcr/run-stats)
          env   (pcrs/run-stats-env stats)]
      (is (= (-> (psm/smart-map env {:com.wsscode.pathom3.attribute/attribute :error})
                 ::pcrs/attribute-error)
             {::pcr/node-error       error
              ::pcrs/node-error-type ::pcrs/node-error-type-direct})))))
