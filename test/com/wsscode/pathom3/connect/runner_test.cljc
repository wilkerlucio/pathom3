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
                   {:buz ::pco/unknown-value})
                 ::p.ent/entity-tree* deref)
             {:foo "bar"})))

    (testing "dont override current data"
      (is (= (-> (pcr/merge-resolver-response!
                   (p.ent/with-entity
                     {::p.path/path []
                      ::pcp/graph   {::pcp/nodes     {}
                                     ::pcp/index-ast {}}}
                     {:foo "bar"})
                   {:foo "other"
                    :buz "baz"})
                 ::p.ent/entity-tree* deref)
             {:foo "bar"
              :buz "baz"})))))

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
                       ::pcr/node-run-stats* (volatile! ^::map-container? {}))]
           (pcr/run-node! env (pcp/get-root-node graph))
           @(::p.ent/entity-tree* env))
         {::geo/left       10
          ::geo/width      30
          ::geo/right      40
          ::geo/half-width 15
          ::geo/center-x   25})))

(defn run-graph [env query tree]
  (let [ast    (eql/query->ast query)
        result (pcr/run-graph! env ast (p.ent/create-entity tree))]
    result))

(defn coords-resolver [c]
  (pco/resolver 'coords-resolver {::pco/output [::coords]}
    (fn [_ _] {::coords c})))

(def full-env (pci/register [geo/full-registry]))

(deftest run-graph!-test
  (is (= (run-graph (pci/register geo/registry)
                    [::geo/right ::geo/center-x]
                    {::geo/left 10 ::geo/width 30})
         {::geo/left       10
          ::geo/width      30
          ::geo/right      40
          ::geo/half-width 15
          ::geo/center-x   25}))

  (is (= (run-graph full-env
                    [{:data [:left]}]
                    {:data {::geo/x 10}})
         {:data {::geo/x    10
                 ::geo/left 10
                 :left      10}}))

  (testing "ident"
    (is (= (run-graph full-env
                      [[::geo/x 10]]
                      {})
           {[::geo/x 10] {::geo/x 10}}))

    (is (= (run-graph full-env
                      [{[::geo/x 10] [::geo/left]}]
                      {})
           {[::geo/x 10] {::geo/x    10
                          ::geo/left 10}}))

    (is (= (run-graph full-env
                      [{[::geo/x 10] [::geo/left]}]
                      {[::geo/x 10] {:random "data"}})
           {[::geo/x 10] {:random    "data"
                          ::geo/x    10
                          ::geo/left 10}})))

  (testing "path"
    (is (= (run-graph (pci/register [(pbir/constantly-resolver ::hold {})
                                     (pbir/constantly-resolver ::sequence [{} {}])
                                     (pbir/constantly-fn-resolver ::p.path/path ::p.path/path)])
                      [::p.path/path
                       {::hold [::p.path/path]}
                       {::sequence [::p.path/path]}]
                      {})
           {::p.path/path [],
            ::sequence    [{::p.path/path [::sequence 0]}
                           {::p.path/path [::sequence 1]}],
            ::hold        {::p.path/path [::hold]}}))

    (testing "map container path"
      (is (= (run-graph (pci/register [(pbir/constantly-resolver ::map-container
                                                                 ^::pcr/map-container? {:foo {}})
                                       (pbir/constantly-fn-resolver ::p.path/path ::p.path/path)])
                        [{::map-container [::p.path/path]}]
                        {})
             {::map-container {:foo {::p.path/path [::map-container :foo]}}}))))

  (testing "insufficient data"
    (let [res (run-graph (pci/register [(pco/resolver 'a {::pco/output [:a]
                                                          ::pco/input  [:b]}
                                          (fn [_ _] {:a "a"}))
                                        (pco/resolver 'b {::pco/output [:b]}
                                          (fn [_ _] {}))])
                         [:a]
                         {})]
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
             {:available {}
              :required  {:b {}}}))))

  (testing "processing sequence of consistent elements"
    (is (= (run-graph (pci/register [geo/full-registry
                                     (coords-resolver
                                       [{::geo/x 7 ::geo/y 9}
                                        {::geo/x 3 ::geo/y 4}])])
                      [{::coords [:left]}]
                      {})
           {::coords [{::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                      {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}]}))

    (testing "data from join"
      (is (= (run-graph (pci/register geo/full-registry)
                        [{::coords [:left]}]
                        {::coords [{::geo/x 7 ::geo/y 9}
                                   {::geo/x 3 ::geo/y 4}]})
             {::coords [{::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                        {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}]})))

    (testing "set data from join"
      (is (= (run-graph (pci/register geo/full-registry)
                        [{::coords [:left]}]
                        {::coords #{{::geo/x 7 ::geo/y 9}
                                    {::geo/x 3 ::geo/y 4}}})
             {::coords #{{::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                         {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}}})))

    (testing "map values"
      (is (= (run-graph (pci/register geo/full-registry)
                        [{::coords [:left]}]
                        {::coords ^::pcr/map-container? {:a {::geo/x 7 ::geo/y 9}
                                                         :b {::geo/x 3 ::geo/y 4}}})
             {::coords {:a {::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                        :b {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}}}))

      (is (= (run-graph (pci/register geo/full-registry)
                        '[{(::coords {::pcr/map-container? true}) [:left]}]
                        {::coords {:a {::geo/x 7 ::geo/y 9}
                                   :b {::geo/x 3 ::geo/y 4}}})
             {::coords {:a {::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                        :b {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}}}))))

  (testing "processing sequence of inconsistent maps"
    (is (= (run-graph (pci/register geo/full-registry)
                      [{::coords [:left]}]
                      {::coords [{::geo/x 7 ::geo/y 9}
                                 {::geo/left 7 ::geo/y 9}]})
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
                      [{::coords [:left]}]
                      {::coords [{::geo/x 7 ::geo/y 9}
                                 20]})
           {::coords [{::geo/x    7
                       ::geo/y    9
                       ::geo/left 7
                       :left      7}
                      20]}))))

(deftest run-graph!-or-test
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
                          [:error]
                          {})
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
                          [:error]
                          {})
               {:error "value"}))
        (is (= @spy 1))))))

(deftest run-graph!-unions-test
  (is (= (run-graph
           (pci/register
             [(pbir/constantly-resolver :list
                                        [{:user/id 123}
                                         {:video/id 2}])
              (pbir/static-attribute-map-resolver :user/id :user/name
                {123 "U"})
              (pbir/static-attribute-map-resolver :video/id :video/title
                {2 "V"})])
           [{:list
             {:user/id  [:user/name]
              :video/id [:video/title]}}]
           {})
         {:list
          [{:user/id 123 :user/name "U"}
           {:video/id 2 :video/title "V"}]})))

(deftest run-graph!-nested-inputs-test
  (testing "data from resolvers"
    (is (= (run-graph
             (pci/register
               [(pco/resolver 'users
                  {::pco/output [{:users [:user/id]}]}
                  (fn [_ _]
                    {:users [{:user/id 1}
                             {:user/id 2}]}))
                (pbir/static-attribute-map-resolver :user/id :user/score
                  {1 10
                   2 20})
                (pco/resolver 'total-score
                  {::pco/input  [{:users [:user/score]}]
                   ::pco/output [:total-score]}
                  (fn [_ {:keys [users]}]
                    {:total-score (reduce + 0 (map :user/score users))}))])
             [:total-score]
             {})
           {:users       [#:user{:id 1, :score 10} #:user{:id 2, :score 20}]
            :total-score 30})))

  (testing "resolver gets only the exact shape it asked for"
    (is (= (run-graph
             (pci/register
               [(pco/resolver 'users
                  {::pco/output [{:users [:user/id]}]}
                  (fn [_ _]
                    {:users [{:user/id 1}
                             {:user/id 2}]}))
                (pbir/static-attribute-map-resolver :user/id :user/score
                  {1 10
                   2 20})
                (pco/resolver 'total-score
                  {::pco/input  [{:users [:user/score]}]
                   ::pco/output [:filter-test]}
                  (fn [_ {:keys [users]}]
                    {:filter-test users}))])
             [:filter-test]
             {})
           {:users       [#:user{:id 1, :score 10} #:user{:id 2, :score 20}]
            :filter-test [#:user{:score 10} #:user{:score 20}]})))

  (testing "source data in available data"
    (is (= (run-graph
             (pci/register
               [(pco/resolver 'total-score
                  {::pco/input  [{:users [:user/score]}]
                   ::pco/output [:total-score]}
                  (fn [_ {:keys [users]}]
                    {:total-score (reduce + 0 (map :user/score users))}))])
             [:total-score]
             {:users [{:user/score 10}
                      {:user/score 20}]})
           {:users       [#:user{:score 10} #:user{:score 20}]
            :total-score 30})))

  (testing "source data partially in available data"
    (is (= (run-graph
             (pci/register
               [(pbir/static-attribute-map-resolver :user/id :user/score
                  {1 10
                   2 20})
                (pco/resolver 'total-score
                  {::pco/input  [{:users [:user/score]}]
                   ::pco/output [:total-score]}
                  (fn [_ {:keys [users]}]
                    {:total-score (reduce + 0 (map :user/score users))}))])
             [:total-score]
             {:users [{:user/id 1}
                      {:user/id 2}]})
           {:users       [#:user{:id 1, :score 10} #:user{:id 2, :score 20}]
            :total-score 30}))))

(deftest run-graph!-optional-inputs-test
  (testing "data from resolvers"
    (is (= (run-graph
             (pci/register
               [(pco/resolver 'foo
                  {::pco/input  [:x (pco/? :y)]
                   ::pco/output [:foo]}
                  (fn [_ {:keys [x y]}]
                    {:foo (if y y x)}))
                (pbir/constantly-resolver :x 10)])
             [:foo]
             {})
           {:x   10
            :foo 10})))

  (testing "data from resolvers"
    (is (= (run-graph
             (pci/register
               [(pco/resolver 'foo
                  {::pco/input  [:x (pco/? :y)]
                   ::pco/output [:foo]}
                  (fn [_ {:keys [x y]}]
                    {:foo (if y y x)}))
                (pbir/constantly-resolver :x 10)
                (pbir/constantly-resolver :y 42)])
             [:foo]
             {})
           {:x   10
            :y   42
            :foo 42})))

  (testing "all optionals"
    (testing "not available"
      (is (= (run-graph
               (pci/register
                 [(pco/resolver 'foo
                    {::pco/input  [(pco/? :y)]
                     ::pco/output [:foo]}
                    (fn [_ {:keys [y]}]
                      {:foo (if y y "nope")}))])
               [:foo]
               {})
             {:foo "nope"})))

    (testing "available"
      (is (= (run-graph
               (pci/register
                 [(pco/resolver 'foo
                    {::pco/input  [(pco/? :y)]
                     ::pco/output [:foo]}
                    (fn [_ {:keys [y]}]
                      {:foo (if y y "nope")}))
                  (pbir/constantly-resolver :y 42)])
               [:foo]
               {})
             {:y   42
              :foo 42})))))

(pco/defresolver batch-fetch [items]
  {::pco/input  [:id]
   ::pco/output [:v]
   ::pco/batch? true}
  (mapv #(hash-map :v (* 10 (:id %))) items))

(pco/defresolver batch-param [env items]
  {::pco/input  [:id]
   ::pco/output [:v-param]
   ::pco/batch? true}
  (let [m (-> (pco/params env) :multiplier (or 10))]
    (mapv #(hash-map :v (* m (:id %))) items)))

(pco/defresolver batch-fetch-error []
  {::pco/input  [:id]
   ::pco/output [:v]
   ::pco/batch? true}
  (throw (ex-info "Batch error" {})))

(pco/defresolver batch-fetch-nested [items]
  {::pco/input  [:id]
   ::pco/output [{:n [:pre-id]}]
   ::pco/batch? true}
  (mapv #(hash-map :n {:pre-id (* 10 (:id %))}) items))

(pco/defresolver batch-pre-id [items]
  {::pco/input  [:pre-id]
   ::pco/output [:id]
   ::pco/batch? true}
  (mapv #(hash-map :id (inc (:pre-id %))) items))

(pco/defresolver pre-idc [items]
  {::pco/input  [:id]
   ::pco/output [:v]
   ::pco/batch? true}
  (mapv #(hash-map :v (* 10 (:id %))) items))

(deftest run-graph!-batch-test
  (testing "simple batching"
    (is (= (run-graph
             (pci/register
               [batch-fetch])
             [{:list [:v]}]
             {:list
              [{:id 1}
               {:id 2}
               {:id 3}]})
           {:list
            [{:id 1 :v 10}
             {:id 2 :v 20}
             {:id 3 :v 30}]})))

  (testing "root batch"
    (is (= (run-graph
             (pci/register
               [batch-fetch])
             [:v]
             {:id 1})
           {:id 1 :v 10}))

    (is (some?
          (-> (run-graph
                (pci/register
                  [batch-fetch])
                [:v]
                {:id 1}) meta ::pcr/run-stats))))

  (testing "params"
    (is (= (run-graph
             (pci/register [batch-param])
             [{:list [:v-param]}]
             {:list
              [{:id 1}
               {:id 2}
               {:id 3}]})
           {:list
            [{:id 1 :v 10}
             {:id 2 :v 20}
             {:id 3 :v 30}]}))

    (is (= (run-graph
             (pci/register [batch-param])
             '[{:list [(:v-param {:multiplier 100})]}]
             {:list
              [{:id 1}
               {:id 2}
               {:id 3}]})
           {:list
            [{:id 1 :v 100}
             {:id 2 :v 200}
             {:id 3 :v 300}]})))

  (testing "run stats"
    (is (some? (-> (run-graph
                     (pci/register
                       [batch-fetch])
                     [{'(:>/id {:id 1}) [:v]}]
                     {})
                   :>/id meta ::pcr/run-stats))))

  (testing "different plan"
    (is (= (run-graph
             (pci/register
               [batch-fetch
                (pbir/constantly-resolver :list
                                          [{:id 1}
                                           {:id 2 :v 200}
                                           {:id 3}])])
             [{:list [:v]}]
             {})
           {:list
            [{:id 1 :v 10}
             {:id 2 :v 200}
             {:id 3 :v 30}]})))

  (testing "multiple batches"
    (is (= (run-graph
             (pci/register
               [batch-fetch
                batch-pre-id])
             [{:list [:v]}]
             {:list
              [{:pre-id 1}
               {:pre-id 2}
               {:id 3}]})
           {:list
            [{:pre-id 1 :id 2 :v 20}
             {:pre-id 2 :id 3 :v 30}
             {:id 3 :v 30}]})))

  (testing "non batching dependency"
    (is (= (run-graph
             (pci/register
               [batch-fetch
                (pbir/single-attr-resolver :pre-id :id inc)])
             [{:list [:v]}]
             {:list
              [{:pre-id 1}
               {:pre-id 2}
               {:id 4}]})
           {:list
            [{:pre-id 1 :id 2 :v 20}
             {:pre-id 2 :id 3 :v 30}
             {:id 4 :v 40}]})))

  (testing "process after batch"
    (testing "deep process"
      (is (= (run-graph
               (pci/register
                 [batch-fetch
                  batch-fetch-nested
                  (pbir/single-attr-resolver :pre-id :id inc)])
               [{:list [{:n [:v]}]}]
               {:list
                [{:id 1}
                 {:id 2}
                 {:id 3}]})
             {:list
              [{:id 1, :n {:pre-id 10 :id 11 :v 110}}
               {:id 2, :n {:pre-id 20 :id 21 :v 210}}
               {:id 3, :n {:pre-id 30 :id 31 :v 310}}]})))

    (testing "node sequence"
      (is (= (run-graph
               (pci/register
                 [batch-fetch
                  (pbir/single-attr-resolver :v :x #(* 100 %))])
               [{:list [:x]}]
               {:list
                [{:id 1}
                 {:id 2}
                 {:id 3}]})
             {:list
              [{:id 1 :v 10 :x 1000}
               {:id 2 :v 20 :x 2000}
               {:id 3 :v 30 :x 3000}]}))))

  (testing "deep batching"
    (is (= (run-graph
             (pci/register
               [batch-fetch])
             [{:list [{:items [:v]}]}]
             {:list
              [{:items [{:id 1}
                        {:id 2}]}
               {:items [{:id 3}
                        {:id 4}]}
               {:items [{:id 5}
                        {:id 6}]}]})
           {:list [{:items [{:id 1, :v 10} {:id 2, :v 20}]}
                   {:items [{:id 3, :v 30} {:id 4, :v 40}]}
                   {:items [{:id 5, :v 50} {:id 6, :v 60}]}]})))

  (testing "errors"
    (let [res (run-graph
                (pci/register
                  [batch-fetch-error
                   (pbir/constantly-resolver :list
                                             [{:id 1}
                                              {:id 2}
                                              {:id 3}])])
                [:v]
                {:id 1})]
      (is (= res
             {:id 1}))
      ; TODO: match error
      #_(is (= (meta res)
               {})))

    (testing "partial error")))

(def mock-todos-db
  [{::todo-message "Write demo on params"
    ::todo-done?   true}
   {::todo-message "Pathom in Rust"
    ::todo-done?   false}])

(defn filter-params-match [env coll]
  (let [params     (pco/params env)
        param-keys (keys params)]
    (if (seq params)
      (filter
        #(= params
            (select-keys % param-keys))
        coll)
      coll)))

(pco/defresolver todos-resolver [env _]
  {::pco/output
   [{::todos
     [::todo-message
      ::todo-done?]}]}
  {::todos (filter-params-match env mock-todos-db)})

(deftest run-graph!-params-test
  (is (= (run-graph
           (pci/register todos-resolver)
           [::todos]
           {})
         {::todos [{::todo-message "Write demo on params"
                    ::todo-done?   true}
                   {::todo-message "Pathom in Rust"
                    ::todo-done?   false}]}))

  (is (= (run-graph
           (pci/register todos-resolver)
           '[(::todos {::todo-done? true})]
           {})
         {::todos [{::todo-message "Write demo on params"
                    ::todo-done?   true}]})))

(deftest run-graph!-cache-test
  (testing "store result in cache"
    (let [cache* (atom {})]
      (is (= (run-graph
               (-> (pci/register
                     [(pbir/constantly-resolver :x 10)
                      (pbir/single-attr-resolver :x :y #(* 2 %))])
                   (assoc ::pcr/resolver-cache* cache*))
               [:y]
               {})
             {:x 10
              :y 20}))
      (is (= @cache*
             '{[x->y-single-attr-transform {:x 10} {}] {:y 20}})))

    (testing "with params"
      (let [cache* (atom {})]
        (is (= (run-graph
                 (-> (pci/register
                       [(pbir/constantly-resolver :x 10)
                        (pbir/single-attr-resolver :x :y #(* 2 %))])
                     (assoc ::pcr/resolver-cache* cache*))
                 ['(:y {:foo "bar"})]
                 {})
               {:x 10
                :y 20}))
        (is (= @cache*
               '{[x->y-single-attr-transform {:x 10} {:foo "bar"}] {:y 20}}))))

    (testing "custom cache key"
      (let [cache*    (atom {})
            my-cache* (atom {})]
        (is (= (run-graph
                 (-> (pci/register
                       [(pbir/constantly-resolver :x 10)
                        (-> (pbir/single-attr-resolver :x :y #(* 2 %))
                            (pco/update-config assoc ::pco/cache-store ::my-cache))])
                     (assoc ::pcr/resolver-cache* cache*)
                     (assoc ::my-cache my-cache*))
                 [:y]
                 {})
               {:x 10
                :y 20}))
        (is (= @cache*
               '{}))
        (is (= @my-cache*
               '{[x->y-single-attr-transform {:x 10} {}] {:y 20}})))))

  (testing "cache hit"
    (is (= (run-graph
             (-> (pci/register
                   [(pbir/constantly-resolver :x 10)
                    (pbir/single-attr-resolver :x :y #(* 2 %))])
                 (assoc ::pcr/resolver-cache* (atom {'[x->y-single-attr-transform {:x 10} {}] {:y 30}})))
             [:y]
             {})
           {:x 10
            :y 30})))

  (testing "cache don't hit with different params"
    (let [cache* (atom {'[x->y-single-attr-transform {:x 10} {}] {:y 30}})]
      (is (= (run-graph
               (-> (pci/register
                     [(pbir/constantly-resolver :x 10)
                      (pbir/single-attr-resolver :x :y #(* 2 %))])
                   (assoc ::pcr/resolver-cache* cache*))
               ['(:y {:z 42})]
               {})
             {:x 10
              :y 20}))

      (is (= @cache*
             {'[x->y-single-attr-transform {:x 10} {}]      {:y 30}
              '[x->y-single-attr-transform {:x 10} {:z 42}] {:y 20}}))))

  (testing "resolver with cache disabled"
    (is (= (run-graph
             (-> (pci/register
                   [(pbir/constantly-resolver :x 10)
                    (assoc-in (pbir/single-attr-resolver :x :y #(* 2 %))
                      [:config ::pco/cache?] false)])
                 (assoc ::pcr/resolver-cache* (atom {'[x->y-single-attr-transform {:x 10}] {:y 30}})))
             [:y]
             {})
           {:x 10
            :y 20}))))

(deftest run-graph!-placeholders-test
  (is (= (run-graph (pci/register (pbir/constantly-resolver :foo "bar"))
           [{:>/path [:foo]}]
           {})
         {:foo    "bar"
          :>/path {:foo "bar"}}))

  (is (= (run-graph (pci/register (pbir/constantly-resolver :foo "bar"))
           [{:>/path [:foo]}]
           {:foo "baz"})
         {:foo    "baz"
          :>/path {:foo "baz"}}))

  (testing "modified data"
    (is (= (run-graph (pci/register
                        [(pbir/single-attr-resolver :x :y #(* 2 %))])
             '[{(:>/path {:x 20}) [:y]}]
             {})
           {:>/path {:x 20
                     :y 40}}))

    (is (= (run-graph (pci/register
                        [(pbir/constantly-resolver :x 10)
                         (pbir/single-attr-resolver :x :y #(* 2 %))])
             '[{(:>/path {:x 20}) [:y]}]
             {})
           {:x      10
            :y      20
            :>/path {:x 20
                     :y 40}}))

    (is (= (run-graph (pci/register
                        [(pbir/constantly-resolver :x 10)
                         (pbir/single-attr-resolver :x :y #(* 2 %))])
             '[:x
               {(:>/path {:x 20}) [:y]}]
             {})
           {:x      10
            :y      20
            :>/path {:x 20
                     :y 40}}))

    (testing "different parameters"
      (is (= (run-graph (pci/register
                          [(pbir/constantly-resolver :x 10)
                           (pbir/single-attr-with-env-resolver :x :y #(* (:m (pco/params %) 2) %2))])
               '[:x
                 {:>/m2 [(:y)]}
                 {:>/m3 [(:y {:m 3})]}
                 {:>/m4 [(:y {:m 4})]}]
               {})
             {:x    10
              :y    20
              :>/m2 {:x 10
                     :y 20}
              :>/m3 {:x 10
                     :y 30}
              :>/m4 {:x 10
                     :y 40}})))))

(deftest run-graph!-mutations-test
  (testing "simple call"
    (is (= (run-graph (pci/register (pco/mutation 'call {}
                                      (fn [_ {:keys [this]}] {:result this})))
             '[(call {:this "thing"})]
             {})
           '{call {:result "thing"}})))

  (testing "mutation join"
    (is (= (run-graph
             (pci/register
               [(pbir/alias-resolver :result :other)
                (pco/mutation 'call {}
                  (fn [_ {:keys [this]}] {:result this}))])
             '[{(call {:this "thing"}) [:other]}]
             {})
           '{call {:result "thing"
                   :other  "thing"}})))

  (testing "mutation error"
    (let [err (ex-info "Error" {})]
      (is (= (run-graph
               (pci/register
                 [(pbir/alias-resolver :result :other)
                  (pco/mutation 'call {}
                    (fn [_ _] (throw err)))])
               '[{(call {:this "thing"}) [:other]}]
               {})
             {'call {::pcr/mutation-error err}}))))

  (testing "mutations run before anything else"
    (is (= (run-graph
             (-> (pci/register
                   [(pbir/constantly-fn-resolver ::env-var (comp deref ::env-var))
                    (pco/mutation 'call {} (fn [{::keys [env-var]} _] (swap! env-var inc)))])
                 (assoc ::env-var (atom 0)))
             '[::env-var
               (call)]
             {})
           {::env-var 1
            'call     1}))))

(deftest run-graph!-errors-test
  (let [error (ex-info "Error" {})
        stats (-> (run-graph (pci/register
                               (pco/resolver 'error {::pco/output [:error]}
                                 (fn [_ _] (throw error))))
                    [:error]
                    {})
                  meta ::pcr/run-stats)
        env   (pcrs/run-stats-env stats)]
    (is (= (-> (psm/smart-map env {:com.wsscode.pathom3.attribute/attribute :error})
               ::pcrs/attribute-error)
           {::pcr/node-error       error
            ::pcrs/node-error-type ::pcrs/node-error-type-direct}))))

(deftest placeholder-merge-entity-test
  ; TODO: currently not possible, need to handle conflicts before
  #_(testing "forward current entity data"
      (is (= (pcr/placeholder-merge-entity
               {::pcp/graph          {::pcp/nodes        {}
                                      ::pcp/placeholders #{:>/p1}
                                      ::pcp/index-ast    {:>/p1 {:key          :>/p1
                                                                 :dispatch-key :>/p1}}}
                ::p.ent/entity-tree* (volatile! {:foo "bar"})}
               {})
             {:>/p1 {:foo "bar"}})))

  (testing "override with source when params are provided"
    (is (= (pcr/placeholder-merge-entity
             {::pcp/graph          {::pcp/nodes        {}
                                    ::pcp/placeholders #{:>/p1}
                                    ::pcp/index-ast    {:>/p1 {:key          :>/p1
                                                               :dispatch-key :>/p1
                                                               :params       {:x 10}}}}
              ::p.ent/entity-tree* (volatile! {:x 20 :y 40 :z true})}
             {:z true})
           {:>/p1 {:z true :x 10}}))))
