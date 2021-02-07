(ns com.wsscode.pathom3.connect.runner-test
  (:require
    [clojure.spec.alpha :as s]
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.connect.runner.async :as pcra]
    [com.wsscode.pathom3.connect.runner.stats :as pcrs]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.interface.smart-map :as psm]
    [com.wsscode.pathom3.path :as p.path]
    [com.wsscode.pathom3.plugin :as p.plugin]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]
    [com.wsscode.promesa.macros :refer [clet]]
    [edn-query-language.core :as eql]
    [matcher-combinators.matchers :as m]
    [matcher-combinators.standalone :as mcs]
    [matcher-combinators.test]
    [promesa.core :as p]))

(defn match-keys? [ks]
  (fn [m]
    (reduce
      (fn [_ k]
        (if-let [v (find m k)]
          (if (s/valid? k (val v))
            true
            (reduced false))
          (reduced false)))
      true
      ks)))

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

(defn run-graph [env tree query]
  (let [ast (eql/query->ast query)]
    (pcr/run-graph! env ast (p.ent/create-entity tree))))

(defn run-graph-async [env tree query]
  (let [ast (eql/query->ast query)]
    (pcra/run-graph! env ast (p.ent/create-entity tree))))

(defn coords-resolver [c]
  (pco/resolver 'coords-resolver {::pco/output [::coords]}
    (fn [_ _] {::coords c})))

(def full-env (pci/register [geo/full-registry]))

(defn graph-response? [env tree query expected]
  (if (fn? expected)
    #_{:clj-kondo/ignore [:single-logical-operand]}
    (let [sync          (expected (run-graph env tree query))
          async #?(:clj (expected @(run-graph-async env tree query)) :cljs true)]
      (and sync
           async))
    (= (run-graph env tree query)
       #?(:clj (let [res @(run-graph-async env tree query)]
                 ;(println res)
                 res))
       expected)))

(comment
  (time
    @(run-graph-async (pci/register
                        (pbir/single-attr-resolver :id :x #(p/delay 100 (inc %))))
       {:items [{:id 1} {:id 2} {:id 3}]}
       [{:items [:x]}])))

(deftest run-graph!-test
  (is (graph-response? (pci/register geo/registry)
                       {::geo/left 10 ::geo/width 30}
                       [::geo/right ::geo/center-x]

                       {::geo/left       10
                        ::geo/width      30
                        ::geo/right      40
                        ::geo/half-width 15
                        ::geo/center-x   25}))

  (is (graph-response? full-env
                       {:data {::geo/x 10}}
                       [{:data [:left]}]
                       {:data {::geo/x    10
                               ::geo/left 10
                               :left      10}}))

  (testing "ident"
    (is (graph-response? full-env
                         {}
                         [[::geo/x 10]]
                         {[::geo/x 10] {::geo/x 10}}))

    (is (graph-response? full-env
                         {}
                         [{[::geo/x 10] [::geo/left]}]
                         {[::geo/x 10] {::geo/x    10
                                        ::geo/left 10}}))

    (is (graph-response? full-env
                         {[::geo/x 10] {:random "data"}}
                         [{[::geo/x 10] [::geo/left]}]
                         {[::geo/x 10] {:random    "data"
                                        ::geo/x    10
                                        ::geo/left 10}})))

  (testing "path"
    (is (graph-response? (pci/register [(pbir/constantly-resolver ::hold {})
                                        (pbir/constantly-resolver ::sequence [{} {}])
                                        (pbir/constantly-fn-resolver ::p.path/path ::p.path/path)])
                         {}
                         [::p.path/path
                          {::hold [::p.path/path]}
                          {::sequence [::p.path/path]}]
                         {::p.path/path [],
                          ::sequence    [{::p.path/path [::sequence 0]}
                                         {::p.path/path [::sequence 1]}],
                          ::hold        {::p.path/path [::hold]}}))

    (testing "map container path"
      (is (graph-response? (pci/register [(pbir/constantly-resolver ::map-container
                                                                    ^::pcr/map-container? {:foo {}})
                                          (pbir/constantly-fn-resolver ::p.path/path ::p.path/path)])
                           {}
                           [{::map-container [::p.path/path]}]
                           {::map-container {:foo {::p.path/path [::map-container :foo]}}}))))

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
             {:available {}
              :required  {:b {}}}))))

  (testing "ending values"
    (let [res (run-graph (pci/register [(pco/resolver 'a {::pco/output [{:a [:n]}]}
                                          (fn [_ _] {:a '({:n 1} {:n 2})}))])
                         {}
                         [{:a [:n]}])]
      (is (= res {:a '({:n 1} {:n 2})}))))

  (testing "processing sequence of consistent elements"
    (is (graph-response? (pci/register [geo/full-registry
                                        (coords-resolver
                                          [{::geo/x 7 ::geo/y 9}
                                           {::geo/x 3 ::geo/y 4}])])
                         {}
                         [{::coords [:left]}]
                         {::coords [{::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                                    {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}]}))

    (testing "data from join"
      (is (graph-response? (pci/register geo/full-registry)
                           {::coords [{::geo/x 7 ::geo/y 9}
                                      {::geo/x 3 ::geo/y 4}]}
                           [{::coords [:left]}]
                           {::coords [{::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                                      {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}]})))

    (testing "set data from join"
      (is (graph-response? (pci/register geo/full-registry)
                           {::coords #{{::geo/x 7 ::geo/y 9}
                                       {::geo/x 3 ::geo/y 4}}}
                           [{::coords [:left]}]
                           {::coords #{{::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                                       {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}}})))

    (testing "map values"
      (is (graph-response? (pci/register geo/full-registry)
                           {::coords ^::pcr/map-container? {:a {::geo/x 7 ::geo/y 9}
                                                            :b {::geo/x 3 ::geo/y 4}}}
                           [{::coords [:left]}]
                           {::coords {:a {::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                                      :b {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}}}))

      (is (graph-response? (pci/register geo/full-registry)
                           {::coords {:a {::geo/x 7 ::geo/y 9}
                                      :b {::geo/x 3 ::geo/y 4}}}
                           '[{(::coords {::pcr/map-container? true}) [:left]}]
                           {::coords {:a {::geo/x 7 ::geo/y 9 ::geo/left 7 :left 7}
                                      :b {::geo/x 3 ::geo/y 4 ::geo/left 3 :left 3}}}))))

  (testing "processing sequence of inconsistent maps"
    (is (graph-response? (pci/register geo/full-registry)
                         {::coords [{::geo/x 7 ::geo/y 9}
                                    {::geo/left 7 ::geo/y 9}]}
                         [{::coords [:left]}]
                         {::coords
                          [{::geo/x    7
                            ::geo/y    9
                            ::geo/left 7
                            :left      7}
                           {::geo/left 7
                            ::geo/y    9
                            :left      7}]})))

  (testing "processing sequence partial items being maps"
    (is (graph-response? (pci/register geo/full-registry)
                         {::coords [{::geo/x 7 ::geo/y 9}
                                    20]}
                         [{::coords [:left]}]
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
                                           {::pco/output [:value]}
                                           (fn [_ _]
                                             (swap! spy inc)
                                             {:value 1}))
                                         (pco/resolver `value2
                                           {::pco/output [:value]}
                                           (fn [_ _]
                                             (swap! spy inc)
                                             {:value 2}))])
                          {}
                          [:value])
               {:value 1}))
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
        (is (= @spy 1))))

    (testing "custom prioritization"
      (is (graph-response?
            (pci/register
              {::pcr/choose-path
               (fn [_env _or-node options]
                 (last options))}
              [(pco/resolver `value
                 {::pco/output [:value]}
                 (fn [_ _]
                   {:value 1}))
               (pco/resolver `value2
                 {::pco/output [:value]}
                 (fn [_ _]
                   {:value 2}))])
            {}
            [:value]
            {:value 2})))

    (testing "stats"
      (is (graph-response?
            (pci/register [(pco/resolver `value
                             {::pco/output [:value]}
                             (fn [_ _]
                               {:value 1}))
                           (pco/resolver `value2
                             {::pco/output [:value]}
                             (fn [_ _]
                               {:value 2}))])
            {}
            [:value]
            (fn [res]
              (mcs/match?
                {::pcr/taken-paths  #{1}
                 ::pcr/success-path 1}
                (-> res meta ::pcr/run-stats ::pcr/node-run-stats (get 3)))))))

    (testing "standard priority"
      (is (graph-response?
            (pci/register
              [(pco/resolver `value
                 {::pco/output [:value]}
                 (fn [_ _]
                   {:value 1}))
               (pco/resolver `value2
                 {::pco/output   [:value]
                  ::pco/priority 1}
                 (fn [_ _]
                   {:value 2}))])
            {}
            [:value]
            {:value 2}))

      (testing "distinct inputs"
        (is (graph-response?
              (pci/register
                [(pco/resolver `value
                   {::pco/input  [:a]
                    ::pco/output [:value]}
                   (fn [_ _]
                     {:value 1}))
                 (pco/resolver `value2
                   {::pco/input    [:b]
                    ::pco/output   [:value]
                    ::pco/priority 1}
                   (fn [_ _]
                     {:value 2}))
                 (pbir/constantly-resolver :a 1)
                 (pbir/constantly-resolver :b 2)])
              {}
              [:value]
              {:b     2
               :value 2}))

        (testing "complex extension"
          (is (graph-response?
                (pci/register
                  [(pco/resolver `value
                     {::pco/input  [:a :b]
                      ::pco/output [:value]}
                     (fn [_ _]
                       {:value 1}))
                   (pco/resolver `value2
                     {::pco/input    [:c]
                      ::pco/output   [:value]
                      ::pco/priority 1}
                     (fn [_ _]
                       {:value 2}))
                   (pbir/constantly-resolver :a 1)
                   (pbir/constantly-resolver :b 2)
                   (pbir/constantly-resolver :c 3)])
                {}
                [:value]
                {:c     3
                 :value 2})))

        (testing "priority in the middle"
          (is (graph-response?
                (pci/register
                  [(pco/resolver `value
                     {::pco/input  [:a :b]
                      ::pco/output [:value]}
                     (fn [_ _]
                       {:value 1}))
                   (pco/resolver `value2
                     {::pco/input  [:c]
                      ::pco/output [:value]}
                     (fn [_ _]
                       {:value 2}))
                   (pbir/constantly-resolver :a 1)
                   (pbir/constantly-resolver :b 2)
                   (pco/update-config
                     (pbir/constantly-resolver :c 3)
                     assoc ::pco/priority 1)])
                {}
                [:value]
                {:c     3
                 :value 2})))

        (testing "leaf is a branch"
          (is (graph-response?
                (pci/register
                  [(pco/resolver `value-b1
                     {::pco/input  [:b]
                      ::pco/output [:a]}
                     (fn [_ _]
                       {:a "b1"}))
                   (pco/resolver `value-b2
                     {::pco/input    [:b]
                      ::pco/output   [:a]
                      ::pco/priority 1}
                     (fn [_ _]
                       {:a "b2"}))
                   (pco/resolver `value-cd
                     {::pco/input  [:c :d]
                      ::pco/output [:a]}
                     (fn [_ _]
                       {:a "cd"}))
                   (pbir/constantly-resolver :b 2)
                   (pbir/alias-resolver :d :c)
                   (pbir/constantly-resolver :d 10)])
                {}
                [:a]
                {:a "b2"
                 :b 2})))))))

(deftest run-graph!-and-test
  (testing "stats"
    (is (graph-response?
          (pci/register [(pco/resolver `value
                           {::pco/output [:value]}
                           (fn [_ _]
                             {:value 1}))
                         (pco/resolver `value2
                           {::pco/output [:value2]}
                           (fn [_ _]
                             {:value2 2}))])
          {}
          [:value :value2]
          {:value 1 :value2 2}))))

(deftest run-graph!-unions-test
  (is (graph-response?
        (pci/register
          [(pbir/constantly-resolver :list
                                     [{:user/id 123}
                                      {:video/id 2}])
           (pbir/static-attribute-map-resolver :user/id :user/name
             {123 "U"})
           (pbir/static-attribute-map-resolver :video/id :video/title
             {2 "V"})])
        {}
        [{:list
          {:user/id  [:user/name]
           :video/id [:video/title]}}]
        {:list
         [{:user/id 123 :user/name "U"}
          {:video/id 2 :video/title "V"}]})))

(deftest run-graph!-nested-inputs-test
  (testing "data from resolvers"
    (is (graph-response?
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
          {}
          [:total-score]
          {:users       [#:user{:id 1, :score 10} #:user{:id 2, :score 20}]
           :total-score 30})))

  (testing "resolver gets only the exact shape it asked for"
    (is (graph-response?
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
          {}
          [:filter-test]
          {:users       [#:user{:id 1, :score 10} #:user{:id 2, :score 20}]
           :filter-test [#:user{:score 10} #:user{:score 20}]})))

  (testing "source data in available data"
    (is (graph-response?
          (pci/register
            [(pco/resolver 'total-score
               {::pco/input  [{:users [:user/score]}]
                ::pco/output [:total-score]}
               (fn [_ {:keys [users]}]
                 {:total-score (reduce + 0 (map :user/score users))}))])
          {:users [{:user/score 10}
                   {:user/score 20}]}
          [:total-score]
          {:users       [#:user{:score 10} #:user{:score 20}]
           :total-score 30})))

  (testing "source data partially in available data"
    (is (graph-response?
          (pci/register
            [(pbir/static-attribute-map-resolver :user/id :user/score
               {1 10
                2 20})
             (pco/resolver 'total-score
               {::pco/input  [{:users [:user/score]}]
                ::pco/output [:total-score]}
               (fn [_ {:keys [users]}]
                 {:total-score (reduce + 0 (map :user/score users))}))])
          {:users [{:user/id 1}
                   {:user/id 2}]}
          [:total-score]
          {:users       [#:user{:id 1, :score 10} #:user{:id 2, :score 20}]
           :total-score 30})))

  (testing "deep nesting"
    (is (graph-response?
          (pci/register
            [(pbir/static-table-resolver :user/id
                                         {1 {:user/scores [{:user/score 10}]}
                                          2 {:user/scores [{:user/score 20}]}})
             (pco/resolver 'total-score
               {::pco/input  [{:users [{:user/scores [:user/score]}]}]
                ::pco/output [:total-score]}
               (fn [_ {:keys [users]}]
                 {:total-score (reduce + 0 (->> users
                                                (mapcat :user/scores)
                                                (map :user/score)))}))])
          {:users [{:user/id 1}
                   {:user/id 2}]}
          [:total-score]
          {:users       [#:user{:id 1, :scores [#:user{:score 10}]}
                         #:user{:id 2, :scores [#:user{:score 20}]}],
           :total-score 30})))

  (testing "nested + batch"
    (is (graph-response?
          (pci/register
            [(pco/resolver 'users
               {::pco/output [{:users [:user/id]}]}
               (fn [_ _]
                 {:users [{:user/id 1}
                          {:user/id 2}]}))
             (pco/resolver 'score-from-id
               {::pco/input  [:user/id]
                ::pco/output [:user/score]
                ::pco/batch? true}
               (fn [_ items]
                 (mapv #(hash-map :user/score (* 10 (:user/id %))) items)))
             (pco/resolver 'total-score
               {::pco/input  [{:users [:user/score]}]
                ::pco/output [:total-score]}
               (fn [_ {:keys [users]}]
                 {:total-score (reduce + 0 (map :user/score users))}))])
          {}
          [:total-score]
          {:users       [#:user{:id 1, :score 10} #:user{:id 2, :score 20}]
           :total-score 30}))

    (is (testing "waiting not at root"
          (graph-response?
            (pci/register
              [(pco/resolver 'users
                 {::pco/output [{:main-db {:users [:user/id]}}]}
                 (fn [_ _]
                   {:main-db
                    {:users [{:user/id 1}
                             {:user/id 2}]}}))
               (pco/resolver 'score-from-id
                 {::pco/input  [:user/id]
                  ::pco/output [:user/score]
                  ::pco/batch? true}
                 (fn [_ items]
                   (mapv #(hash-map :user/score (* 10 (:user/id %))) items)))
               (pco/resolver 'total-score
                 {::pco/input  [{:users [:user/score]}]
                  ::pco/output [:total-score]}
                 (fn [_ {:keys [users]}]
                   {:total-score (reduce + 0 (map :user/score users))}))])
            {}
            [{:main-db [:total-score]}]
            {:main-db
             {:users [#:user{:id 1, :score 10} #:user{:id 2, :score 20}],
              :total-score 30}})))))

(deftest run-graph!-optional-inputs-test
  (testing "data from resolvers"
    (is (graph-response?
          (pci/register
            [(pco/resolver 'foo
               {::pco/input  [:x (pco/? :y)]
                ::pco/output [:foo]}
               (fn [_ {:keys [x y]}]
                 {:foo (if y y x)}))
             (pbir/constantly-resolver :x 10)])
          {}
          [:foo]
          {:x   10
           :foo 10})))

  (testing "data from resolvers"
    (is (graph-response?
          (pci/register
            [(pco/resolver 'foo
               {::pco/input  [:x (pco/? :y)]
                ::pco/output [:foo]}
               (fn [_ {:keys [x y]}]
                 {:foo (if y y x)}))
             (pbir/constantly-resolver :x 10)
             (pbir/constantly-resolver :y 42)])
          {}
          [:foo]
          {:x   10
           :y   42
           :foo 42})))

  (testing "all optionals"
    (testing "not available"
      (is (graph-response?
            (pci/register
              [(pco/resolver 'foo
                 {::pco/input  [(pco/? :y)]
                  ::pco/output [:foo]}
                 (fn [_ {:keys [y]}]
                   {:foo (if y y "nope")}))])
            {}
            [:foo]
            {:foo "nope"})))

    (testing "available"
      (is (graph-response?
            (pci/register
              [(pco/resolver 'foo
                 {::pco/input  [(pco/? :y)]
                  ::pco/output [:foo]}
                 (fn [_ {:keys [y]}]
                   {:foo (if y y "nope")}))
               (pbir/constantly-resolver :y 42)])
            {}
            [:foo]
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
    (is (graph-response?
          (pci/register
            [batch-fetch])
          {:list
           [{:id 1}
            {:id 2}
            {:id 3}]}
          [{:list [:v]}]
          {:list
           [{:id 1 :v 10}
            {:id 2 :v 20}
            {:id 3 :v 30}]})))

  (testing "root batch"
    (is (graph-response?
          (pci/register
            [batch-fetch])
          {:id 1}
          [:v]
          {:id 1 :v 10}))

    (is (some?
          (-> (run-graph
                (pci/register
                  [batch-fetch])
                {:id 1}
                [:v]) meta ::pcr/run-stats))))

  (testing "params"
    (is (graph-response?
          (pci/register [batch-param])
          {:list
           [{:id 1}
            {:id 2}
            {:id 3}]}
          [{:list [:v-param]}]
          {:list
           [{:id 1 :v 10}
            {:id 2 :v 20}
            {:id 3 :v 30}]}))

    (is (graph-response?
          (pci/register [batch-param])
          {:list
           [{:id 1}
            {:id 2}
            {:id 3}]}
          '[{:list [(:v-param {:multiplier 100})]}]
          {:list
           [{:id 1 :v 100}
            {:id 2 :v 200}
            {:id 3 :v 300}]})))

  (testing "run stats"
    (is (some? (-> (run-graph
                     (pci/register
                       [batch-fetch])
                     {}
                     [{'(:>/id {:id 1}) [:v]}])
                   :>/id meta ::pcr/run-stats))))

  (testing "different plan"
    (is (graph-response?
          (pci/register
            [batch-fetch
             (pbir/constantly-resolver :list
                                       [{:id 1}
                                        {:id 2 :v 200}
                                        {:id 3}])])
          {}
          [{:list [:v]}]
          {:list
           [{:id 1 :v 10}
            {:id 2 :v 200}
            {:id 3 :v 30}]})))

  (testing "multiple batches"
    (is (graph-response?
          (pci/register
            [batch-fetch
             batch-pre-id])
          {:list
           [{:pre-id 1}
            {:pre-id 2}
            {:id 3}]}
          [{:list [:v]}]
          {:list
           [{:pre-id 1 :id 2 :v 20}
            {:pre-id 2 :id 3 :v 30}
            {:id 3 :v 30}]})))

  (testing "non batching dependency"
    (is (graph-response?
          (pci/register
            [batch-fetch
             (pbir/single-attr-resolver :pre-id :id inc)])
          {:list
           [{:pre-id 1}
            {:pre-id 2}
            {:id 4}]}
          [{:list [:v]}]
          {:list
           [{:pre-id 1 :id 2 :v 20}
            {:pre-id 2 :id 3 :v 30}
            {:id 4 :v 40}]})))

  (testing "process after batch"
    (testing "deep process"
      (is (graph-response?
            (pci/register
              [batch-fetch
               batch-fetch-nested
               (pbir/single-attr-resolver :pre-id :id inc)])
            {:list
             [{:id 1}
              {:id 2}
              {:id 3}]}
            [{:list [{:n [:v]}]}]
            {:list
             [{:id 1, :n {:pre-id 10 :id 11 :v 110}}
              {:id 2, :n {:pre-id 20 :id 21 :v 210}}
              {:id 3, :n {:pre-id 30 :id 31 :v 310}}]})))

    (testing "node sequence"
      (is (graph-response?
            (pci/register
              [batch-fetch
               (pbir/single-attr-resolver :v :x #(* 100 %))])
            {:list
             [{:id 1}
              {:id 2}
              {:id 3}]}
            [{:list [:x]}]
            {:list
             [{:id 1 :v 10 :x 1000}
              {:id 2 :v 20 :x 2000}
              {:id 3 :v 30 :x 3000}]}))))

  (testing "deep batching"
    (is (graph-response?
          (pci/register
            [batch-fetch])
          {:list
           [{:items [{:id 1}
                     {:id 2}]}
            {:items [{:id 3}
                     {:id 4}]}
            {:items [{:id 5}
                     {:id 6}]}]}
          [{:list [{:items [:v]}]}]
          {:list [{:items [{:id 1, :v 10} {:id 2, :v 20}]}
                  {:items [{:id 3, :v 30} {:id 4, :v 40}]}
                  {:items [{:id 5, :v 50} {:id 6, :v 60}]}]})))

  (testing "cache"
    (is (graph-response?
          (pci/register
            {::pcr/resolver-cache*
             (volatile!
               {[`batch-fetch {:id 1} {}] {:v 100}
                [`batch-fetch {:id 3} {}] {:v 300}})}
            [batch-fetch])
          {:list
           [{:id 1}
            {:id 2}
            {:id 3}]}
          [{:list [:v]}]
          {:list
           [{:id 1 :v 100}
            {:id 2 :v 20}
            {:id 3 :v 300}]}))

    (testing "custom cache store"
      (is (graph-response?
            (pci/register
              {::custom-cache*
               (volatile!
                 {[`batch-fetch {:id 1} {}] {:v 100}
                  [`batch-fetch {:id 3} {}] {:v 300}})}
              [(pco/update-config batch-fetch
                                  assoc ::pco/cache-store ::custom-cache*)])
            {:list
             [{:id 1}
              {:id 2}
              {:id 3}]}
            [{:list [:v]}]
            {:list
             [{:id 1 :v 100}
              {:id 2 :v 20}
              {:id 3 :v 300}]}))))

  (testing "errors"
    (let [res (run-graph
                (pci/register
                  [batch-fetch-error
                   (pbir/constantly-resolver :list
                                             [{:id 1}
                                              {:id 2}
                                              {:id 3}])])
                {:id 1}
                [:v])]
      (is (= res
             {:id 1}))
      ; TODO: match error
      #_(is (= (meta res)
               {})))

    (testing "partial error"))

  (testing "uses batch resolver as single resolver when running under a path that batch wont work"
    (is (graph-response?
          (pci/register
            [batch-fetch])
          {:list
           #{{:id 1}
             {:id 2}
             {:id 3}}}
          [{:list [:v]}]
          {:list
           #{{:id 1 :v 10}
             {:id 2 :v 20}
             {:id 3 :v 30}}}))))

(deftest run-graph!-run-stats
  (is (graph-response?
        (pci/register
          [(pco/resolver 'a {::pco/output [:a]} (fn [_ _] {:a 1}))
           (pco/resolver 'b {::pco/output [:b]
                             ::pco/input  [:a]} (fn [_ _] {:b 2}))])
        {}
        [:b]
        (fn [res]
          (let [stats (-> res meta ::pcr/run-stats)]
            (and (= (-> (get-in stats [::pcr/node-run-stats 2])
                        (select-keys [::pcr/node-resolver-input
                                      ::pcr/node-resolver-output]))
                    {::pcr/node-resolver-input  {}
                     ::pcr/node-resolver-output {:a 1}})
                 (= (-> (get-in stats [::pcr/node-run-stats 1])
                        (select-keys [::pcr/node-resolver-input
                                      ::pcr/node-resolver-output]))
                    {::pcr/node-resolver-input  {:a 1}
                     ::pcr/node-resolver-output {:b 2}}))))))

  (is (graph-response?
        (pci/register
          {::pcr/run-stats-omit-resolver-io? true}
          [(pco/resolver 'a {::pco/output [:a]} (fn [_ _] {:a 1}))
           (pco/resolver 'b {::pco/output [:b]
                             ::pco/input  [:a]} (fn [_ _] {:b 2}))])
        {}
        [:b]
        (fn [res]
          (let [stats (-> res meta ::pcr/run-stats)]
            (and (= (-> (get-in stats [::pcr/node-run-stats 2])
                        (select-keys [::pcr/node-resolver-input-shape
                                      ::pcr/node-resolver-output-shape]))
                    {::pcr/node-resolver-input-shape  {}
                     ::pcr/node-resolver-output-shape {:a {}}})
                 (= (-> (get-in stats [::pcr/node-run-stats 1])
                        (select-keys [::pcr/node-resolver-input-shape
                                      ::pcr/node-resolver-output-shape]))
                    {::pcr/node-resolver-input-shape  {:a {}}
                     ::pcr/node-resolver-output-shape {:b {}}}))))))

  (testing "error"
    (is (graph-response?
          (pci/register
            [(pco/resolver 'a {::pco/output [:x]}
               (fn [_ _] (throw (ex-info "Err" {}))))])
          {}
          [:x]
          #(mcs/match?
             {::pcr/node-run-stats {1 {::pcr/node-error             any?
                                       ::pcr/node-resolver-input    map?
                                       ::pcr/node-resolver-output   any?
                                       ::pcr/node-run-finish-ms     number?
                                       ::pcr/node-run-start-ms      number?
                                       ::pcr/resolver-run-finish-ms number?
                                       ::pcr/resolver-run-start-ms  number?}}}
             (-> % meta ::pcr/run-stats)))))

  (testing "batch"
    (is (graph-response?
          (pci/register
            [(pco/resolver 'a {::pco/input  [:id]
                               ::pco/output [:x]
                               ::pco/batch? true}
               (fn [_ items] (mapv #(array-map :x (inc (:id %))) items)))])
          {:items [{:id 1} {:id 2}]}
          [{:items [:x]}]
          #(mcs/match?
             {::pcr/node-run-stats {1 {::pcr/node-run-start-ms      number?
                                       ::pcr/node-run-finish-ms     number?
                                       ::pcr/resolver-run-start-ms  number?
                                       ::pcr/resolver-run-finish-ms number?
                                       ::pcr/node-resolver-output   any?
                                       ::pcr/node-resolver-input    map?
                                       ::pcr/batch-run-start-ms     number?
                                       ::pcr/batch-run-finish-ms    number?}}}
             (-> % :items first meta ::pcr/run-stats))))

    (is (graph-response?
          (pci/register
            {::pcr/run-stats-omit-resolver-io? true}
            [(pco/resolver 'a {::pco/input  [:id]
                               ::pco/output [:x]
                               ::pco/batch? true}
               (fn [_ items] (mapv #(array-map :x (inc (:id %))) items)))])
          {:items [{:id 1} {:id 2}]}
          [{:items [:x]}]
          #(mcs/match?
             {::pcr/node-run-stats {1 {::pcr/node-run-start-ms          number?
                                       ::pcr/node-run-finish-ms         number?
                                       ::pcr/resolver-run-start-ms      number?
                                       ::pcr/resolver-run-finish-ms     number?
                                       ::pcr/node-resolver-output       m/absent
                                       ::pcr/node-resolver-input        m/absent
                                       ::pcr/node-resolver-output-shape any?
                                       ::pcr/node-resolver-input-shape  map?
                                       ::pcr/batch-run-start-ms         number?
                                       ::pcr/batch-run-finish-ms        number?}}}
             (-> % :items first meta ::pcr/run-stats)))))

  (testing "mutations"
    (is (graph-response?
          (pci/register
            [(pco/mutation 'call {}
               (fn [_ {:keys [this]}] {:result this}))])
          {}
          ['(call {})]
          #(mcs/match?
             {::pcr/node-run-stats {'call (match-keys? [::pcr/mutation-run-start-ms
                                                        ::pcr/mutation-run-finish-ms
                                                        ::pcr/node-run-start-ms
                                                        ::pcr/node-run-finish-ms])}}
             (-> % meta ::pcr/run-stats))))))

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
  (is (graph-response?
        (pci/register todos-resolver)
        {}
        [::todos]
        {::todos [{::todo-message "Write demo on params"
                   ::todo-done?   true}
                  {::todo-message "Pathom in Rust"
                   ::todo-done?   false}]}))

  (is (graph-response?
        (pci/register todos-resolver)
        {}
        '[(::todos {::todo-done? true})]
        {::todos [{::todo-message "Write demo on params"
                   ::todo-done?   true}]})))

(deftest run-graph!-cache-test
  (testing "store result in cache"
    (let [cache* (atom {})]
      (is (graph-response?
            (-> (pci/register
                  [(pbir/constantly-resolver :x 10)
                   (pbir/single-attr-resolver :x :y #(* 2 %))])
                (assoc ::pcr/resolver-cache* cache*))
            {}
            [:y]
            {:x 10
             :y 20}))
      (is (= @cache*
             '{[x->y-single-attr-transform {:x 10} {}] {:y 20}})))

    (testing "with params"
      (let [cache* (atom {})]
        (is (graph-response?
              (-> (pci/register
                    [(pbir/constantly-resolver :x 10)
                     (pbir/single-attr-resolver :x :y #(* 2 %))])
                  (assoc ::pcr/resolver-cache* cache*))
              {}
              ['(:y {:foo "bar"})]
              {:x 10
               :y 20}))
        (is (= @cache*
               '{[x->y-single-attr-transform {:x 10} {:foo "bar"}] {:y 20}}))))

    (testing "custom cache key"
      (let [cache*    (atom {})
            my-cache* (atom {})]
        (is (graph-response?
              (-> (pci/register
                    [(pbir/constantly-resolver :x 10)
                     (-> (pbir/single-attr-resolver :x :y #(* 2 %))
                         (pco/update-config assoc ::pco/cache-store ::my-cache))])
                  (assoc ::pcr/resolver-cache* cache*)
                  (assoc ::my-cache my-cache*))
              {}
              [:y]
              {:x 10
               :y 20}))
        (is (= @cache*
               '{}))
        (is (= @my-cache*
               '{[x->y-single-attr-transform {:x 10} {}] {:y 20}})))))

  (testing "cache hit"
    (is (graph-response?
          (-> (pci/register
                [(pbir/constantly-resolver :x 10)
                 (pbir/single-attr-resolver :x :y #(* 2 %))])
              (assoc ::pcr/resolver-cache* (atom {'[x->y-single-attr-transform {:x 10} {}] {:y 30}})))
          {}
          [:y]
          {:x 10
           :y 30})))

  (testing "cache don't hit with different params"
    (let [cache* (atom {'[x->y-single-attr-transform {:x 10} {}] {:y 30}})]
      (is (graph-response?
            (-> (pci/register
                  [(pbir/constantly-resolver :x 10)
                   (pbir/single-attr-resolver :x :y #(* 2 %))])
                (assoc ::pcr/resolver-cache* cache*))
            {}
            ['(:y {:z 42})]
            {:x 10
             :y 20}))

      (is (= @cache*
             {'[x->y-single-attr-transform {:x 10} {}]      {:y 30}
              '[x->y-single-attr-transform {:x 10} {:z 42}] {:y 20}}))))

  (testing "resolver with cache disabled"
    (is (graph-response?
          (-> (pci/register
                [(pbir/constantly-resolver :x 10)
                 (assoc-in (pbir/single-attr-resolver :x :y #(* 2 %))
                   [:config ::pco/cache?] false)])
              (assoc ::pcr/resolver-cache* (atom {'[x->y-single-attr-transform {:x 10}] {:y 30}})))
          {}
          [:y]
          {:x 10
           :y 20}))))

(deftest run-graph!-placeholders-test
  (is (graph-response? (pci/register (pbir/constantly-resolver :foo "bar"))
        {}
        [{:>/path [:foo]}]
        {:foo    "bar"
         :>/path {:foo "bar"}}))

  (is (graph-response? (pci/register (pbir/constantly-resolver :foo "bar"))
        {:foo "baz"}
        [{:>/path [:foo]}]
        {:foo    "baz"
         :>/path {:foo "baz"}}))

  (testing "modified data"
    (is (graph-response? (pci/register
                           [(pbir/single-attr-resolver :x :y #(* 2 %))])
          {}
          '[{(:>/path {:x 20}) [:y]}]
          {:>/path {:x 20
                    :y 40}}))

    (is (graph-response? (pci/register
                           [(pbir/constantly-resolver :x 10)
                            (pbir/single-attr-resolver :x :y #(* 2 %))])
          {}
          '[{(:>/path {:x 20}) [:y]}]
          {:x      10
           :y      20
           :>/path {:x 20
                    :y 40}}))

    (is (graph-response? (pci/register
                           [(pbir/constantly-resolver :x 10)
                            (pbir/single-attr-resolver :x :y #(* 2 %))])
          {}
          '[:x
            {(:>/path {:x 20}) [:y]}]
          {:x      10
           :y      20
           :>/path {:x 20
                    :y 40}}))

    (testing "different parameters"
      (is (graph-response? (pci/register
                             [(pbir/constantly-resolver :x 10)
                              (pbir/single-attr-with-env-resolver :x :y #(* (:m (pco/params %) 2) %2))])
            {}
            '[:x
              {:>/m2 [(:y)]}
              {:>/m3 [(:y {:m 3})]}
              {:>/m4 [(:y {:m 4})]}]
            {:x    10
             :y    20
             :>/m2 {:x 10
                    :y 20}
             :>/m3 {:x 10
                    :y 30}
             :>/m4 {:x 10
                    :y 40}})))))

(deftest run-graph!-recursive-test
  (testing "unbounded recursive query"
    (is (graph-response?
          (pci/register
            [(pbir/static-table-resolver :name
               {"a" {:children [{:name "b"}
                                {:name "c"}]}
                "b" {:children [{:name "e"}]}
                "e" {:children [{:name "f"}]}
                "f" {:children [{:name "g"}]}
                "c" {:children [{:name "d"}]}})])
          {:name "a"}
          [:name
           {:children '...}]
          {:name     "a",
           :children [{:name     "b",
                       :children [{:name "e", :children [{:name "f", :children [{:name "g"}]}]}]}
                      {:name "c", :children [{:name "d"}]}]})))

  (testing "bounded recursive query"
    (is (graph-response?
          (pci/register
            [(pbir/static-table-resolver :name
               {"a" {:children [{:name "b"}
                                {:name "c"}]}
                "b" {:children [{:name "e"}]}
                "e" {:children [{:name "f"}]}
                "f" {:children [{:name "g"}]}
                "c" {:children [{:name "d"}]}})])
          {:name "a"}
          [:name
           {:children 2}]
          {:name     "a",
           :children [{:name "b", :children [{:name "e", :children [{:name "f"}]}]}
                      {:name "c", :children [{:name "d"}]}]}))

    (is (graph-response?
          (pci/register
            [(pbir/static-table-resolver :name
               {"a" {:children [{:name "b"}
                                {:name "c"}]}
                "b" {:children [{:name "e"}]}
                "e" {:children [{:name "f"}]}
                "f" {:children [{:name "g"}]}
                "c" {:children [{:name "d"}]}})
             (pbir/single-attr-resolver :name :name+ #(str % "+"))])
          {:name "a"}
          [:name+
           {:children 1}]
          {:name     "a",
           :name+    "a+",
           :children [{:name "b", :name+ "b+", :children [{:name "e"}]}
                      {:name "c", :name+ "c+", :children [{:name "d"}]}]})))

  (testing "recursion over known data"
    (is (graph-response?
          (pci/register
            [(pbir/single-attr-resolver :name :name+ #(str % "+"))])
          {:name     "a",
           :children [{:name     "b",
                       :children [{:name "e"}]}
                      {:name "c"}]}
          [:name+
           {:children '...}]
          {:name     "a"
           :name+    "a+"
           :children [{:name     "b"
                       :name+    "b+"
                       :children [{:name  "e"
                                   :name+ "e+"}]}
                      {:name  "c"
                       :name+ "c+"}]})))

  (testing "recursive nested input"
    (is (graph-response?
          (pci/register
            [(pco/resolver 'nested-input-recursive
               {::pco/input  [:name {:children '...}]
                ::pco/output [:names]}
               (fn [_ input]
                 {:names
                  (mapv :name (tree-seq :children :children input))}))
             (pbir/static-table-resolver :name
               {"a" {:children [{:name "b"}
                                {:name "c"}]}
                "b" {:children [{:name "e"}]}
                "e" {:children [{:name "f"}]}
                "f" {:children [{:name "g"}]}
                "c" {:children [{:name "d"}]}})])
          {:name "a"}
          [:names]
          {:name     "a",
           :children [{:name     "b",
                       :children [{:name     "e",
                                   :children [{:name     "f",
                                               :children [{:name "g"}],
                                               :names    ["f" "g"]}],
                                   :names    ["e" "f" "g"]}],
                       :names    ["b" "e" "f" "g"]}
                      {:name "c", :children [{:name "d"}], :names ["c" "d"]}],
           :names    ["a" "b" "e" "f" "g" "c" "d"]}))))

(deftest run-graph!-mutations-test
  (testing "simple call"
    (is (graph-response? (pci/register (pco/mutation 'call {}
                                         (fn [_ {:keys [this]}] {:result this})))
          {}
          '[(call {:this "thing"})]
          '{call {:result "thing"}})))

  (testing "mutation join"
    (is (graph-response?
          (pci/register
            [(pbir/alias-resolver :result :other)
             (pco/mutation 'call {}
               (fn [_ {:keys [this]}] {:result this}))])
          {}
          '[{(call {:this "thing"}) [:other]}]
          '{call {:result "thing"
                  :other  "thing"}})))

  (testing "mutation error"
    (let [err (ex-info "Error" {})]
      (is (graph-response?
            (pci/register
              [(pbir/alias-resolver :result :other)
               (pco/mutation 'call {}
                 (fn [_ _] (throw err)))])
            {}
            '[{(call {:this "thing"}) [:other]}]
            {'call {::pcr/mutation-error err}}))))

  (testing "mutation not found"
    (is (graph-response?
          (pci/register
            [(pbir/alias-resolver :result :other)])
          {}
          '[(not-here {:this "thing"})]
          (fn [res]
            (mcs/match?
              {'not-here {::pcr/mutation-error (fn [e]
                                                 (and (= "Mutation not found"
                                                         (ex-message e))
                                                      (= {::pco/op-name 'not-here}
                                                         (ex-data e))))}}
              res)))))

  (testing "mutations run before anything else"
    (is (graph-response?
          (-> (pci/register
                [(pbir/constantly-fn-resolver ::env-var (comp deref ::env-var))
                 (pco/mutation 'call {} (fn [{::keys [env-var]} _] (swap! env-var inc)))])
              (assoc ::env-var (atom 0)))
          {}
          '[::env-var
            (call)]
          #(= (::env-var %) (get % 'call))))))

(deftest run-graph!-errors-test
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

#?(:clj
   (deftest run-graph!-async-tests
     (is (= @(run-graph-async (pci/register
                                (pco/resolver 'async {::pco/output [:foo]}
                                  (fn [_ _] (p/resolved {:foo "foo"}))))
               {}
               [:foo])
            {:foo "foo"}))

     (testing "error"
       (let [error (ex-info "Error" {})
             stats @(run-graph-async (pci/register
                                       (pco/resolver 'error {::pco/output [:error]}
                                         (fn [_ _] (p/resolved (throw error)))))
                      {}
                      [:error])
             env   (pcrs/run-stats-env (-> stats meta ::pcr/run-stats))]
         (is (= (-> (psm/smart-map env {:com.wsscode.pathom3.attribute/attribute :error})
                    ::pcrs/attribute-error)
                {::pcr/node-error       error
                 ::pcrs/node-error-type ::pcrs/node-error-type-direct}))))))

(defn set-done [k]
  (fn [process]
    (fn [env ast-or-graph entity-tree*]
      (clet [res (process env ast-or-graph entity-tree*)]
        (assoc res k true)))))

(deftest plugin-extensions-tests
  (testing "wrap graph execution"
    (is (graph-response? (p.plugin/register
                           [{::p.plugin/id         'wrap
                             ::pcr/wrap-run-graph! (set-done :done?)}])
          {:foo {:bar "baz"}}
          [{:foo [:bar]}]
          {:foo {:bar "baz", :done? true}, :done? true})))

  (testing "wrap graph root execution"
    (is (graph-response? (p.plugin/register
                           [{::p.plugin/id              'wrap
                             ::pcr/wrap-root-run-graph! (set-done :done?)}])
          {:foo {:bar "baz"}}
          [{:foo [:bar]}]
          {:foo {:bar "baz"}, :done? true}))))
