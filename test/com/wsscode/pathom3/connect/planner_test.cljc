(ns com.wsscode.pathom3.connect.planner-test
  (:require
    [check.core :refer [check =>]]
    [clojure.test :refer [deftest is testing]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.foreign :as pcf]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.error :as p.error]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [edn-query-language.core :as eql]))

(defn register-index [ops]
  (let [ops' (into []
                   (map (fn [x]
                          (if (and (coll/native-map? x) (contains? x ::pco/output))
                            (pco/resolver (assoc x ::pco/resolve (fn [_ _])))
                            x)))
                   ops)]
    (pci/register {} ops')))

(defn oir-index [resolvers]
  (::pci/index-oir (register-index resolvers)))

(defn base-graph-env []
  (pcp/base-env))

(defn compute-run-graph* [{::keys [out env]}]
  (pcp/compute-run-graph
    out
    env))

(defn compute-env
  [{::keys     [resolvers dynamics]
    ::pcp/keys [snapshots*]
    ::eql/keys [query]
    :as        options}]
  (cond-> (merge (base-graph-env)
                 (-> options
                     (dissoc ::eql/query)
                     (assoc :edn-query-language.ast/node
                       (eql/query->ast query))
                     (cond->
                       (::pci/index-resolvers options)
                       (update ::pci/index-resolvers
                         #(coll/map-vals pco/resolver %)))))
    resolvers
    (pci/merge-indexes (register-index resolvers))

    snapshots*
    (assoc ::pcp/snapshots* snapshots*)

    dynamics
    (as-> <>
      (reduce
        (fn [env' [name resolvers]]
          (pci/register env' (-> resolvers
                                 register-index
                                 (assoc ::pci/index-source-id name)
                                 p.eql/boundary-interface
                                 pcf/foreign-register)))
        <>
        dynamics))))

(defn compute-run-graph
  [{::keys     [time?]
    ::pcp/keys [snapshots*]
    :or        {time? false}
    :as        options}]
  (let [env     (compute-env options)
        options (assoc options ::env env)
        graph   (cond->
                  (if time?
                    (time (compute-run-graph* options))
                    (compute-run-graph* options))

                  true
                  (-> (vary-meta assoc ::env env)
                      (dissoc ::pcp/source-ast ::pcp/available-data)))]
    (if snapshots*
      @snapshots*
      graph)))

#?(:clj
   (defn debug-compute-run-graph
     "Use this to run plan graph and also log the steps to Pathom Viz"
     [options]
     (let [env   (compute-env options)
           snaps (pcp/compute-plan-snapshots env)]

       ((requiring-resolve 'com.wsscode.pathom.viz.ws-connector.pathom3/log-entry)
        {:pathom.viz.log/type :pathom.viz.log.type/plan-snapshots
         :pathom.viz.log/data snaps})
       (-> (peek snaps)
           (dissoc ::pcp/snapshot-message)
           (vary-meta assoc ::env env)
           (dissoc ::pcp/source-ast ::pcp/available-data)))))

(deftest compute-run-graph-test
  (testing "simplest path"
    (is (= (compute-run-graph
             {::resolvers [{::pco/op-name 'a
                            ::pco/output  [:a]}]
              ::eql/query [:a]})
           '{::pcp/nodes                 {1 {::pco/op-name a
                                             ::pcp/node-id 1
                                             ::pcp/expects {:a {}}
                                             ::pcp/input   {}}}
             ::pcp/index-resolver->nodes {a #{1}}
             ::pcp/root                  1
             ::pcp/index-attrs           {:a #{1}}
             ::pcp/index-ast             {:a {:dispatch-key :a
                                              :key          :a
                                              :type         :prop}}})))

  (testing "OR on multiple paths"
    (is (= (compute-run-graph
             {::resolvers [{::pco/op-name 'a
                            ::pco/output  [:a]}
                           {::pco/op-name 'a2
                            ::pco/output  [:a]}]
              ::eql/query [:a]})
           '{::pcp/nodes                 {2 {::pco/op-name      a2,
                                             ::pcp/expects      {:a {}},
                                             ::pcp/input        {},
                                             ::pcp/node-id      2,
                                             ::pcp/node-parents #{3}},
                                          1 {::pco/op-name      a,
                                             ::pcp/expects      {:a {}},
                                             ::pcp/input        {},
                                             ::pcp/node-id      1,
                                             ::pcp/node-parents #{3}},
                                          3 {::pcp/expects {:a {}},
                                             ::pcp/node-id 3,
                                             ::pcp/run-or  #{1
                                                             2}}},
             ::pcp/index-ast             {:a {:type         :prop,
                                              :dispatch-key :a,
                                              :key          :a}},
             ::pcp/index-resolver->nodes {a2 #{2}, a #{1}},
             ::pcp/index-attrs           {:a #{1 2}},
             ::pcp/root                  3})))

  (testing "partial failure with OR"
    (is (= (compute-run-graph
             {::pci/index-oir      '{:bar/id     {{:foo/id {}}   #{bar-id}
                                                  {:bar/year {}} #{get-bar-id-from-year}}
                                     :foo/number {{:foo/id {}} #{bar-id}}
                                     :bar/year   {{:bar/id {}} #{get-year}}}
              ::eql/query          [:foo/number :bar/id :bar/year]
              ::pcp/available-data {:foo/id {}}})
           '{::pcp/nodes                 {1 {::pco/op-name  bar-id,
                                             ::pcp/expects  {:foo/number {},
                                                             :bar/id     {}},
                                             ::pcp/input    {:foo/id {}},
                                             ::pcp/node-id  1,
                                             ::pcp/run-next 5},
                                          5 {::pco/op-name      get-year,
                                             ::pcp/expects      {:bar/year {}},
                                             ::pcp/input        {:bar/id {}},
                                             ::pcp/node-id      5,
                                             ::pcp/node-parents #{1}}},
             ::pcp/index-ast             {:foo/number {:type         :prop,
                                                       :dispatch-key :foo/number,
                                                       :key          :foo/number},
                                          :bar/id     {:type         :prop,
                                                       :dispatch-key :bar/id,
                                                       :key          :bar/id},
                                          :bar/year   {:type         :prop,
                                                       :dispatch-key :bar/year,
                                                       :key          :bar/year}},
             ::pcp/index-resolver->nodes {bar-id #{1}, get-year #{5}},
             ::pcp/index-attrs           {:foo/number #{1},
                                          :bar/id     #{1},
                                          :bar/year   #{5}},
             ::pcp/root                  1})))

  (testing "AND on multiple attributes"
    (is (= (compute-run-graph
             {::resolvers [{::pco/op-name 'a
                            ::pco/output  [:a]}
                           {::pco/op-name 'b
                            ::pco/output  [:b]}]
              ::eql/query [:a :b]})
           '{::pcp/nodes                 {1 {::pco/op-name      a
                                             ::pcp/node-id      1
                                             ::pcp/expects      {:a {}}
                                             ::pcp/input        {}
                                             ::pcp/node-parents #{3}}
                                          2 {::pco/op-name      b
                                             ::pcp/node-id      2
                                             ::pcp/expects      {:b {}}
                                             ::pcp/input        {}
                                             ::pcp/node-parents #{3}}
                                          3 {::pcp/node-id 3
                                             ::pcp/run-and #{2 1}}}
             ::pcp/index-resolver->nodes {a #{1} b #{2}}
             ::pcp/index-attrs           {:b #{2}, :a #{1}}
             ::pcp/index-ast             {:a {:type         :prop,
                                              :dispatch-key :a,
                                              :key          :a},
                                          :b {:type         :prop,
                                              :dispatch-key :b,
                                              :key          :b}}
             ::pcp/root                  3})))

  (testing "expectation for nested needs"
    (testing "exposed required sub query"
      (check (=> {::pcp/nodes {1 {::pcp/expects {:a {:b {}}}}}}
                 (compute-run-graph
                   {::resolvers [{::pco/op-name 'a
                                  ::pco/output  [{:a [:b :c]}]}]
                    ::eql/query [{:a [:b]}]}))))

    (testing "filters only parts included on the resolver provides"
      (check (=> {::pcp/nodes {2 {::pcp/expects {:a {:b {}}}}}}
                 (compute-run-graph
                   {::resolvers [{::pco/op-name 'a
                                  ::pco/output  [{:a [:b :c]}]}
                                 {::pco/op-name 'd
                                  ::pco/output  [:d]}]
                    ::eql/query [{:a [:b :d]}]}))))

    (testing "add expects on implicit dependency"
      (check (=> {::pcp/nodes {2 {::pcp/expects {:a {:b {}}}}}}
                 (compute-run-graph
                   {::resolvers [{::pco/op-name 'a
                                  ::pco/output  [{:a [:b :c]}]}
                                 {::pco/op-name 'd
                                  ::pco/input   [:b]
                                  ::pco/output  [:d]}]
                    ::eql/query [{:a [:d]}]}))))))

(deftest compute-run-graph-no-path-test
  (testing "no path"
    (is (thrown-with-msg?
          #?(:clj Throwable :cljs js/Error)
          #"Pathom can't find a path for the following elements in the query: \[:a]"
          (compute-run-graph
            {::pci/index-oir '{}
             ::eql/query     [:a]})))

    (is (thrown-with-msg?
          #?(:clj Throwable :cljs js/Error)
          #"Pathom can't find a path for the following elements in the query: \[:a :b]"
          (compute-run-graph
            {::pci/index-oir '{}
             ::eql/query     [:a :b]})))

    (testing "broken chain"
      (is (thrown-with-msg?
            #?(:clj Throwable :cljs js/Error)
            #"Pathom can't find a path for the following elements in the query: \[:b]"
            (compute-run-graph
              {::pci/index-oir '{:b {{:a {}} #{b}}}
               ::eql/query     [:b]})))

      (is (thrown-with-msg?
            #?(:clj Throwable :cljs js/Error)
            #"Pathom can't find a path for the following elements in the query: \[:b]"
            (compute-run-graph
              {::pci/index-oir '{:b {{:a {}} #{b1 b}}}
               ::eql/query     [:b]})))

      (is (thrown-with-msg?
            #?(:clj Throwable :cljs js/Error)
            #"Pathom can't find a path for the following elements in the query: \[:b]"
            (compute-run-graph
              {::resolvers [{::pco/op-name 'a
                             ::pco/output  [:a]}
                            {::pco/op-name 'b
                             ::pco/input   [:a]
                             ::pco/output  [:b]}]
               ::eql/query [:b]
               ::out       {::pcp/unreachable-paths {:a {}}}})))

      (is (thrown-with-msg?
            #?(:clj Throwable :cljs js/Error)
            #"Pathom can't find a path for the following elements in the query: \[:c]"
            (compute-run-graph
              {::resolvers [{::pco/op-name 'b
                             ::pco/input   [:a]
                             ::pco/output  [:b]}
                            {::pco/op-name 'c
                             ::pco/input   [:b]
                             ::pco/output  [:c]}]
               ::eql/query [:c]})))

      (is (thrown-with-msg?
            #?(:clj Throwable :cljs js/Error)
            #""
            (compute-run-graph
              {::resolvers [{::pco/op-name 'b
                             ::pco/input   [:a]
                             ::pco/output  [:b]}
                            {::pco/op-name 'd
                             ::pco/output  [:d]}
                            {::pco/op-name 'c
                             ::pco/input   [:b :d]
                             ::pco/output  [:c]}]
               ::eql/query [:c]}))))

    (testing "currently available data"
      (is (= (compute-run-graph
               {::pci/index-oir      '{}
                ::eql/query          [:a]
                ::pcp/available-data {:a {}}})
             {::pcp/nodes     {}
              ::pcp/index-ast {:a {:dispatch-key :a
                                   :key          :a
                                   :type         :prop}}}))

      (testing "optional"
        (is (= (compute-run-graph
                 {::resolvers [{::pco/op-name 'a
                                ::pco/output  [:a]}]
                  ::eql/query [:a (pco/? :b)]})
               '{::pcp/nodes                 {1 {::pco/op-name a,
                                                 ::pcp/expects {:a {}},
                                                 ::pcp/input   {},
                                                 ::pcp/node-id 1}},
                 ::pcp/index-ast             {:a {:type         :prop,
                                                  :dispatch-key :a,
                                                  :key          :a},
                                              :b {:type         :prop,
                                                  :dispatch-key :b,
                                                  :key          :b,
                                                  :params       {::pco/optional? true}}},
                 ::pcp/index-resolver->nodes {a #{1}},
                 ::pcp/index-attrs           {:a #{1}},
                 ::pcp/root                  1,
                 ::pcp/unreachable-paths     {:b {}}})))

      (testing "exposed nested needs"
        (is (= (compute-run-graph
                 {::pci/index-oir      '{}
                  ::eql/query          [{:a [:bar]}]
                  ::pcp/available-data {:a {}}})
               {::pcp/nodes          {}
                ::pcp/nested-process #{:a}
                ::pcp/index-ast      {:a {:children     [{:dispatch-key :bar
                                                          :key          :bar
                                                          :type         :prop}]
                                          :dispatch-key :a
                                          :key          :a
                                          :query        [:bar]
                                          :type         :join}}}))

        (is (= (compute-run-graph
                 {::pci/index-oir      '{}
                  ::eql/query          [:a {:b '...}]
                  ::pcp/available-data {:a {} :b {}}})
               {::pcp/nodes          {}
                ::pcp/nested-process #{:b}
                ::pcp/index-ast      {:a {:type         :prop,
                                          :dispatch-key :a,
                                          :key          :a},
                                      :b {:type         :join,
                                          :dispatch-key :b,
                                          :key          :b,
                                          :query        '...}}}))))))

(deftest compute-run-graph-no-path-tolerant-mode-test
  (testing "no path"
    (is (= (compute-run-graph
             {::pci/index-oir         '{}
              ::eql/query             [:a]
              ::p.error/lenient-mode? true})
           {::pcp/index-ast            {:a {:dispatch-key :a
                                            :key          :a
                                            :type         :prop}}
            ::pcp/nodes                {}
            ::pcp/unreachable-paths    {:a {}}
            ::pcp/verification-failed? true}))

    (testing "broken chain"
      (is (= (compute-run-graph
               {::pci/index-oir         '{:b {{:a {}} #{b}}}
                ::eql/query             [:b]
                ::p.error/lenient-mode? true})
             '{::pcp/index-ast            {:b {:dispatch-key :b
                                               :key          :b
                                               :type         :prop}}
               ::pcp/nodes                {}
               ::pcp/unreachable-paths    {:a {}
                                           :b {}}
               ::pcp/verification-failed? true}))

      (is (= (compute-run-graph
               {::pci/index-oir         '{:b {{:a {}} #{b1 b}}}
                ::eql/query             [:b]
                ::p.error/lenient-mode? true})
             '{::pcp/index-ast            {:b {:dispatch-key :b
                                               :key          :b
                                               :type         :prop}}
               ::pcp/nodes                {}
               ::pcp/unreachable-paths    {:a {}
                                           :b {}}
               ::pcp/verification-failed? true}))

      (is (= (compute-run-graph
               {::resolvers             [{::pco/op-name 'a
                                          ::pco/output  [:a]}
                                         {::pco/op-name 'b
                                          ::pco/input   [:a]
                                          ::pco/output  [:b]}]
                ::eql/query             [:b]
                ::out                   {::pcp/unreachable-paths {:a {}}}
                ::p.error/lenient-mode? true})
             '{::pcp/index-ast            {:b {:dispatch-key :b
                                               :key          :b
                                               :type         :prop}}
               ::pcp/nodes                {}
               ::pcp/unreachable-paths    {:a {}
                                           :b {}}
               ::pcp/verification-failed? true}))

      (is (= (compute-run-graph
               {::resolvers             [{::pco/op-name 'b
                                          ::pco/input   [:a]
                                          ::pco/output  [:b]}
                                         {::pco/op-name 'c
                                          ::pco/input   [:b]
                                          ::pco/output  [:c]}]
                ::eql/query             [:c]
                ::p.error/lenient-mode? true})
             '{::pcp/index-ast            {:c {:dispatch-key :c
                                               :key          :c
                                               :type         :prop}}
               ::pcp/nodes                {}
               ::pcp/unreachable-paths    {:a {}
                                           :b {}
                                           :c {}}
               ::pcp/verification-failed? true}))

      (is (= (compute-run-graph
               {::resolvers             [{::pco/op-name 'b
                                          ::pco/input   [:a]
                                          ::pco/output  [:b]}
                                         {::pco/op-name 'd
                                          ::pco/output  [:d]}
                                         {::pco/op-name 'c
                                          ::pco/input   [:b :d]
                                          ::pco/output  [:c]}]
                ::eql/query             [:c]
                ::p.error/lenient-mode? true})
             '{::pcp/index-ast            {:c {:dispatch-key :c
                                               :key          :c
                                               :type         :prop}}
               ::pcp/nodes                {}
               ::pcp/unreachable-paths    {:a {}
                                           :b {}
                                           :c {}}
               ::pcp/verification-failed? true})))))

(deftest compute-run-graph-mutations-test
  (is (= (compute-run-graph
           {::pci/index-oir '{}
            ::eql/query     [(list 'foo {})]})
         '{::pcp/nodes     {}
           ::pcp/mutations [foo]
           ::pcp/index-ast {foo {:dispatch-key foo
                                 :key          foo
                                 :params       {}
                                 :type         :call}}})))

(deftest compute-run-graph-dynamic-mutations-test
  (testing "mutation query going over"
    (testing "attribute directly in mutation output"
      (is (= (compute-run-graph
               {::dynamics  {'dyn [(pco/mutation 'doit {::pco/output [:done]} (fn [_ _]))]}
                ::eql/query [{(list 'doit {}) [:done]}]})
             '{::pcp/nodes     {},
               ::pcp/index-ast {doit {:dispatch-key     doit,
                                      :key              doit,
                                      :params           {},
                                      :type             :call,
                                      :query            [:done],
                                      :children         [{:type         :prop,
                                                          :dispatch-key :done,
                                                          :key          :done}],
                                      ::pcp/foreign-ast {:type     :root,
                                                         :children [{:type         :prop,
                                                                     :key          :done,
                                                                     :dispatch-key :done}]}}},
               ::pcp/mutations [doit]})))

    (testing "attribute extended from mutation, but still in the same foreign"
      (is (= (compute-run-graph
               {::dynamics  {'dyn [(pco/mutation 'doit {::pco/output [:done]} (fn [_ _]))
                                   (pbir/alias-resolver :done :done?)]}
                ::eql/query [{(list 'doit {}) [:done?]}]})
             '{::pcp/nodes     {},
               ::pcp/index-ast {doit {:dispatch-key     doit,
                                      :key              doit,
                                      :params           {},
                                      :type             :call,
                                      :query            [:done?],
                                      :children         [{:type         :prop,
                                                          :dispatch-key :done?,
                                                          :key          :done?}],
                                      ::pcp/foreign-ast {:type     :root,
                                                         :children [{:type         :prop,
                                                                     :key          :done?,
                                                                     :dispatch-key :done?}]}}},
               ::pcp/mutations [doit]})))

    (testing "attribute extended from mutation locally"
      (is (= (compute-run-graph
               {::dynamics  {'dyn [(pco/mutation 'doit {::pco/output [:done]} (fn [_ _]))]}
                ::resolvers [(pbir/alias-resolver :done :done?)]
                ::eql/query [{(list 'doit {}) [:done?]}]})
             '{::pcp/index-ast {doit {:children         [{:dispatch-key :done?
                                                          :key          :done?
                                                          :type         :prop}]
                                      ::pcp/foreign-ast {:children [{:dispatch-key :done
                                                                     :key          :done
                                                                     :type         :prop}]
                                                         :type     :root}
                                      :dispatch-key     doit
                                      :key              doit
                                      :params           {}
                                      :query            [:done?]
                                      :type             :call}}
               ::pcp/mutations [doit]
               ::pcp/nodes     {}})))))

(deftest compute-run-graph-idents-test
  (testing "separate idents"
    (is (= (compute-run-graph
             {::resolvers [{::pco/op-name 'a
                            ::pco/output  [:a]}]
              ::eql/query [:a [:foo "bar"]]})
           '{::pcp/nodes                 {1 {::pco/op-name a
                                             ::pcp/node-id 1
                                             ::pcp/expects {:a {}}
                                             ::pcp/input   {}}}
             ::pcp/index-resolver->nodes {a #{1}}
             ::pcp/root                  1
             ::pcp/idents                #{[:foo "bar"]}
             ::pcp/index-attrs           {:a #{1}}
             ::pcp/index-ast             {:a           {:dispatch-key :a
                                                        :key          :a
                                                        :type         :prop}
                                          [:foo "bar"] {:dispatch-key :foo
                                                        :key          [:foo
                                                                       "bar"]
                                                        :type         :prop}}}))

    (is (= (compute-run-graph
             {::resolvers [{::pco/op-name 'a
                            ::pco/output  [:a]}]
              ::eql/query [:a {[:foo "bar"] [:baz]}]})
           '{::pcp/nodes                 {1 {::pco/op-name a
                                             ::pcp/node-id 1
                                             ::pcp/expects {:a {}}
                                             ::pcp/input   {}}}
             ::pcp/index-resolver->nodes {a #{1}}
             ::pcp/root                  1
             ::pcp/idents                #{[:foo "bar"]}
             ::pcp/index-attrs           {:a #{1}}
             ::pcp/index-ast             {:a           {:dispatch-key :a
                                                        :key          :a
                                                        :type         :prop}
                                          [:foo "bar"] {:children     [{:dispatch-key :baz
                                                                        :key          :baz
                                                                        :type         :prop}]
                                                        :dispatch-key :foo
                                                        :key          [:foo
                                                                       "bar"]
                                                        :query        [:baz]
                                                        :type         :join}}}))))

(deftest compute-run-graph-cycles-test
  (testing "cycles"
    (is (= (compute-run-graph
             {::resolvers             [{::pco/op-name 'a
                                        ::pco/input   [:b]
                                        ::pco/output  [:a]}
                                       {::pco/op-name 'b
                                        ::pco/input   [:a]
                                        ::pco/output  [:b]}]
              ::eql/query             [:a]
              ::p.error/lenient-mode? true})
           '{::pcp/index-ast            {:a {:dispatch-key :a
                                             :key          :a
                                             :type         :prop}}
             ::pcp/nodes                {}
             ::pcp/unreachable-paths    {:a {}
                                         :b {}}
             ::pcp/verification-failed? true}))

    (is (= (compute-run-graph
             {::resolvers             [{::pco/op-name 'a
                                        ::pco/input   [:c]
                                        ::pco/output  [:a]}
                                       {::pco/op-name 'b
                                        ::pco/input   [:a]
                                        ::pco/output  [:b]}
                                       {::pco/op-name 'c
                                        ::pco/input   [:b]
                                        ::pco/output  [:c]}]
              ::eql/query             [:a]
              ::p.error/lenient-mode? true})
           '{::pcp/index-ast            {:a {:dispatch-key :a
                                             :key          :a
                                             :type         :prop}}
             ::pcp/nodes                {}
             ::pcp/unreachable-paths    {:a {}
                                         :b {}
                                         :c {}}
             ::pcp/verification-failed? true}))

    (testing "partial cycle"
      (is (= (compute-run-graph
               {::pci/index-oir '{:a {{:c {}} #{a}
                                      {}      #{a1}}
                                  :b {{:a {}} #{b}}
                                  :c {{:b {}} #{c}}
                                  :d {{} #{d}}}
                ::eql/query     [:c :a]})
             '#::pcp{:index-ast             {:a {:dispatch-key :a
                                                 :key          :a
                                                 :type         :prop}
                                             :c {:dispatch-key :c
                                                 :key          :c
                                                 :type         :prop}}
                     :index-attrs           {:a #{4}
                                             :b #{2}
                                             :c #{1}}
                     :index-resolver->nodes {a1 #{4}
                                             b  #{2}
                                             c  #{1}}
                     :nodes                 {1 {::pco/op-name      c
                                                ::pcp/expects      {:c {}}
                                                ::pcp/input        {:b {}}
                                                ::pcp/node-id      1
                                                ::pcp/node-parents #{2}}
                                             2 {::pco/op-name      b
                                                ::pcp/expects      {:b {}}
                                                ::pcp/input        {:a {}}
                                                ::pcp/node-id      2
                                                ::pcp/node-parents #{4}
                                                ::pcp/run-next     1}
                                             4 {::pco/op-name  a1
                                                ::pcp/expects  {:a {}}
                                                ::pcp/input    {}
                                                ::pcp/node-id  4
                                                ::pcp/run-next 2}}
                     :root                  4})))

    (testing "nested cycles"
      (is (thrown-with-msg?
            #?(:clj Throwable :cljs js/Error)
            #"Pathom can't find a path for the following elements in the query: \[:b]"
            (compute-run-graph
              {::resolvers [{::pco/op-name 'cycle-a
                             ::pco/output  [:a]}
                            {::pco/op-name 'cycle-b
                             ::pco/input   [{:a [:b]}]
                             ::pco/output  [:b]}]
               ::eql/query [:b]})))

      (is (thrown-with-msg?
            #?(:clj Throwable :cljs js/Error)
            #"Pathom can't find a path for the following elements in the query: \[:c]"
            (compute-run-graph
              {::resolvers [{::pco/op-name 'cycle-a
                             ::pco/output  [:a]}
                            {::pco/op-name 'cycle-b
                             ::pco/input   [{:a [:c]}]
                             ::pco/output  [:b]}
                            {::pco/op-name 'cycle-c
                             ::pco/input   [:b]
                             ::pco/output  [:c]}]
               ::eql/query [:b]}))))))

(deftest compute-run-graph-nested-inputs-test
  (testing "discard non available paths on nesting"
    (is (= (compute-run-graph
             (-> {::eql/query             [:scores-sum]
                  ::resolvers             '[{::pco/op-name scores-sum
                                             ::pco/input   [{:users [:user/score]}]
                                             ::pco/output  [:scores-sum]}
                                            {::pco/op-name users
                                             ::pco/output  [{:users [:user/id]}]}]
                  ::p.error/lenient-mode? true}))
           '{::pcp/index-ast            {:scores-sum {:dispatch-key :scores-sum
                                                      :key          :scores-sum
                                                      :type         :prop}}
             ::pcp/nodes                {}
             ::pcp/unreachable-paths    {:scores-sum {}
                                         :users      {:user/score {}}}
             ::pcp/verification-failed? true})))

  (testing "allow possible path"
    (is (= (compute-run-graph
             (-> {::eql/query             [:scores-sum]
                  ::resolvers             '[{::pco/op-name scores-sum
                                             ::pco/input   [{:users [:user/score]}]
                                             ::pco/output  [:scores-sum]}
                                            {::pco/op-name users
                                             ::pco/output  [{:users [:user/id]}]}
                                            {::pco/op-name user
                                             ::pco/input   [:user/id]
                                             ::pco/output  [:user/score]}]
                  ::p.error/lenient-mode? true}))
           '{::pcp/index-ast             {:scores-sum {:dispatch-key :scores-sum
                                                       :key          :scores-sum
                                                       :type         :prop}
                                          :users      {:children     [{:dispatch-key :user/score
                                                                       :key          :user/score
                                                                       :type         :prop}]
                                                       :dispatch-key :users
                                                       :key          :users
                                                       :type         :join}}
             ::pcp/index-attrs           {:scores-sum #{1}
                                          :users      #{3}}
             ::pcp/index-resolver->nodes {scores-sum #{1}
                                          users      #{3}}
             ::pcp/nodes                 {1 {::pco/op-name      scores-sum
                                             ::pcp/expects      {:scores-sum {}}
                                             ::pcp/input        {:users {:user/score {}}}
                                             ::pcp/node-id      1
                                             ::pcp/node-parents #{3}}
                                          3 {::pco/op-name  users
                                             ::pcp/expects  {:users {:user/id {}}}
                                             ::pcp/input    {}
                                             ::pcp/node-id  3
                                             ::pcp/run-next 1}}
             ::pcp/root                  3})))

  (testing "remove bad paths regarding nested inputs"
    (is (= (compute-run-graph
             {::eql/query             [:z]
              ::resolvers             '[{::pco/op-name a1
                                         ::pco/output  [{:a [:c]}]}
                                        {::pco/op-name a2
                                         ::pco/output  [:a]}
                                        {::pco/op-name a3
                                         ::pco/output  [{:a [:b]}]}
                                        {::pco/op-name z
                                         ::pco/input   [{:a [:b]}]
                                         ::pco/output  [:z]}]
              ::p.error/lenient-mode? true})
           '{:com.wsscode.pathom3.connect.planner/index-ast             {:a {:children     [{:dispatch-key :b
                                                                                             :key          :b
                                                                                             :type         :prop}]
                                                                             :dispatch-key :a
                                                                             :key          :a
                                                                             :type         :join}
                                                                         :z {:dispatch-key :z
                                                                             :key          :z
                                                                             :type         :prop}}
             :com.wsscode.pathom3.connect.planner/index-attrs           {:a #{4}
                                                                         :z #{1}}
             :com.wsscode.pathom3.connect.planner/index-resolver->nodes {a3 #{4}
                                                                         z  #{1}}
             :com.wsscode.pathom3.connect.planner/nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    z
                                                                            :com.wsscode.pathom3.connect.planner/expects      {:z {}}
                                                                            :com.wsscode.pathom3.connect.planner/input        {:a {:b {}}}
                                                                            :com.wsscode.pathom3.connect.planner/node-id      1
                                                                            :com.wsscode.pathom3.connect.planner/node-parents #{4}}
                                                                         4 {:com.wsscode.pathom3.connect.operation/op-name a3
                                                                            :com.wsscode.pathom3.connect.planner/expects   {:a {:b {}}}
                                                                            :com.wsscode.pathom3.connect.planner/input     {}
                                                                            :com.wsscode.pathom3.connect.planner/node-id   4
                                                                            :com.wsscode.pathom3.connect.planner/run-next  1}}
             :com.wsscode.pathom3.connect.planner/root                  4})))

  (testing "data partially available, require join lookup"
    (is (= (compute-run-graph
             (-> {::eql/query             [:scores-sum]
                  ::pcp/available-data    {:users {:user/id {}}}
                  ::resolvers             '[{::pco/op-name scores-sum
                                             ::pco/input   [{:users [:user/score]}]
                                             ::pco/output  [:scores-sum]}
                                            {::pco/op-name users
                                             ::pco/output  [{:users [:user/id]}]}
                                            {::pco/op-name user
                                             ::pco/input   [:user/id]
                                             ::pco/output  [:user/score]}]
                  ::p.error/lenient-mode? true}))
           '#::pcp{:nodes                 {1 {::pco/op-name scores-sum,
                                              ::pcp/node-id 1,
                                              ::pcp/expects {:scores-sum {}},
                                              ::pcp/input   {:users #:user{:score {}}}}},
                   :index-resolver->nodes {scores-sum #{1}},
                   :nested-process        #{:users}
                   :index-attrs           {:scores-sum #{1}}
                   :root                  1
                   :index-ast             {:scores-sum {:type         :prop,
                                                        :dispatch-key :scores-sum,
                                                        :key          :scores-sum}
                                           :users      {:type         :join
                                                        :key          :users
                                                        :dispatch-key :users
                                                        :children     [{:type         :prop
                                                                        :key          :user/score
                                                                        :dispatch-key :user/score}]}}})))

  (testing "data partially available, require nested and resolver call"
    (is (= (compute-run-graph
             (-> {::eql/query             [:scores-sum]
                  ::pcp/available-data    {:users {:user/id {}}}
                  ::resolvers             '[{::pco/op-name scores-sum
                                             ::pco/input   [{:users [:user/score]} :other]
                                             ::pco/output  [:scores-sum]}
                                            {::pco/op-name users
                                             ::pco/output  [{:users [:user/id]}]}
                                            {::pco/op-name other
                                             ::pco/output  [:other]}
                                            {::pco/op-name user
                                             ::pco/input   [:user/id]
                                             ::pco/output  [:user/score]}]
                  ::p.error/lenient-mode? true}))
           '#::pcp{:nodes                 {1 {::pco/op-name      scores-sum,
                                              ::pcp/node-id      1,
                                              ::pcp/expects      {:scores-sum {}},
                                              ::pcp/input        {:users #:user{:score {}},
                                                                  :other {}},
                                              ::pcp/node-parents #{3},},
                                           3 {::pco/op-name  other,
                                              ::pcp/node-id  3,
                                              ::pcp/expects  {:other {}},
                                              ::pcp/input    {},
                                              ::pcp/run-next 1}},
                   :index-resolver->nodes {scores-sum #{1},
                                           other      #{3}},
                   :nested-process        #{:users}
                   :index-ast             {:scores-sum {:type         :prop,
                                                        :dispatch-key :scores-sum,
                                                        :key          :scores-sum}
                                           :users      {:type         :join
                                                        :key          :users
                                                        :dispatch-key :users
                                                        :children     [{:type         :prop
                                                                        :key          :user/score
                                                                        :dispatch-key :user/score}]}},
                   :index-attrs           {:other #{3}, :scores-sum #{1}},
                   :root                  3})))

  (testing "data completely available, skip dependency"
    (is (= (compute-run-graph
             (-> {::eql/query             [:scores-sum]
                  ::pcp/available-data    {:users {:user/score {}}}
                  ::resolvers             '[{::pco/op-name scores-sum
                                             ::pco/input   [{:users [:user/score]}]
                                             ::pco/output  [:scores-sum]}
                                            {::pco/op-name users
                                             ::pco/output  [{:users [:user/id]}]}
                                            {::pco/op-name user
                                             ::pco/input   [:user/id]
                                             ::pco/output  [:user/score]}]
                  ::p.error/lenient-mode? true}))
           '{::pcp/nodes
             {1
              {::pco/op-name scores-sum,
               ::pcp/node-id 1,
               ::pcp/expects {:scores-sum {}},
               ::pcp/input
               {:users {:user/score {}}}}},
             ::pcp/index-resolver->nodes
             {scores-sum #{1}},
             ::pcp/index-ast
             {:scores-sum
              {:type :prop, :dispatch-key :scores-sum, :key :scores-sum}},
             ::pcp/root        1,
             ::pcp/index-attrs {:scores-sum #{1}}})))

  (testing "multiple resolvers for the same root but different sub queries"
    (is (= (compute-run-graph
             (-> {::eql/query             [:scores-sum :total-max-score]
                  ::resolvers             '[{::pco/op-name scores-sum
                                             ::pco/input   [{:users [:user/score]}]
                                             ::pco/output  [:scores-sum]}
                                            {::pco/op-name total-max
                                             ::pco/input   [{:users [:user/max-score]}]
                                             ::pco/output  [:total-max-score]}
                                            {::pco/op-name users
                                             ::pco/output  [{:users [:user/id]}]}
                                            {::pco/op-name user
                                             ::pco/input   [:user/id]
                                             ::pco/output  [:user/score]}]
                  ::p.error/lenient-mode? true}))
           '{::pcp/index-ast             {:scores-sum      {:dispatch-key :scores-sum
                                                            :key          :scores-sum
                                                            :type         :prop}
                                          :total-max-score {:dispatch-key :total-max-score
                                                            :key          :total-max-score
                                                            :type         :prop}
                                          :users           {:children     [{:dispatch-key :user/score
                                                                            :key          :user/score
                                                                            :type         :prop}]
                                                            :dispatch-key :users
                                                            :key          :users
                                                            :type         :join}}
             ::pcp/index-attrs           {:scores-sum #{1}
                                          :users      #{3}}
             ::pcp/index-resolver->nodes {scores-sum #{1}
                                          users      #{3}}
             ::pcp/nodes                 {1 {::pco/op-name      scores-sum
                                             ::pcp/expects      {:scores-sum {}}
                                             ::pcp/input        {:users {:user/score {}}}
                                             ::pcp/node-id      1
                                             ::pcp/node-parents #{3}}
                                          3 {::pco/op-name  users
                                             ::pcp/expects  {:users {:user/id {}}}
                                             ::pcp/input    {}
                                             ::pcp/node-id  3
                                             ::pcp/run-next 1}}
             ::pcp/root                  3
             ::pcp/unreachable-paths     {:total-max-score {}
                                          :users           {:user/max-score {}}}
             ::pcp/verification-failed?  true})))

  (testing "self output reference in input"
    (is (= (compute-run-graph
             (-> {::eql/query             [:b]
                  ::resolvers             '[{::pco/op-name x
                                             ::pco/input   [{:a [:b]}]
                                             ::pco/output  [:b]}]
                  ::p.error/lenient-mode? true}))
           '{::pcp/index-ast            {:b {:dispatch-key :b
                                             :key          :b
                                             :type         :prop}}
             ::pcp/nodes                {}
             ::pcp/unreachable-paths    {:a {}
                                         :b {}}
             ::pcp/verification-failed? true}))

    (is (= (compute-run-graph
             (-> {::eql/query             [:b]
                  ::pcp/available-data    {:a {}}
                  ::resolvers             '[{::pco/op-name x
                                             ::pco/input   [{:a [:b]}]
                                             ::pco/output  [:b]}]
                  ::p.error/lenient-mode? true}))
           '{::pcp/index-ast            {:b {:dispatch-key :b
                                             :key          :b
                                             :type         :prop}}
             ::pcp/nodes                {}
             ::pcp/unreachable-paths    {:a {:b {}}
                                         :b {}}
             ::pcp/verification-failed? true})))

  (testing "multiple distinct nested details"
    (is (= (compute-run-graph
             (-> {::eql/query [:scores-sum :age-sum]
                  ::resolvers '[{::pco/op-name scores-sum
                                 ::pco/input   [{:users [:user/score]}]
                                 ::pco/output  [:scores-sum]}
                                {::pco/op-name age-sum
                                 ::pco/input   [{:users [:user/age]}]
                                 ::pco/output  [:age-sum]}
                                {::pco/op-name users
                                 ::pco/output  [{:users [:user/id]}]}
                                {::pco/op-name user
                                 ::pco/input   [:user/id]
                                 ::pco/output  [:user/age :user/score]}]}))
           '{::pcp/index-ast             {:age-sum    {:dispatch-key :age-sum
                                                       :key          :age-sum
                                                       :type         :prop}
                                          :scores-sum {:dispatch-key :scores-sum
                                                       :key          :scores-sum
                                                       :type         :prop}
                                          :users      {:children     [{:dispatch-key :user/score
                                                                       :key          :user/score
                                                                       :type         :prop}
                                                                      {:dispatch-key :user/age
                                                                       :key          :user/age
                                                                       :type         :prop}]
                                                       :dispatch-key :users
                                                       :key          :users
                                                       :type         :join}}
             ::pcp/index-attrs           {:age-sum    #{5}
                                          :scores-sum #{1}
                                          :users      #{3}}
             ::pcp/index-resolver->nodes {age-sum    #{5}
                                          scores-sum #{1}
                                          users      #{3}}
             ::pcp/nodes                 {1  {::pco/op-name      scores-sum
                                              ::pcp/expects      {:scores-sum {}}
                                              ::pcp/input        {:users {:user/score {}}}
                                              ::pcp/node-id      1
                                              ::pcp/node-parents #{11}}
                                          11 {::pcp/node-id      11
                                              ::pcp/node-parents #{3}
                                              ::pcp/run-and      #{1
                                                                   5}}
                                          3  {::pco/op-name  users
                                              ::pcp/expects  {:users {:user/id {}}}
                                              ::pcp/input    {}
                                              ::pcp/node-id  3
                                              ::pcp/run-next 11}
                                          5  {::pco/op-name      age-sum
                                              ::pcp/expects      {:age-sum {}}
                                              ::pcp/input        {:users {:user/age {}}}
                                              ::pcp/node-id      5
                                              ::pcp/node-parents #{11}}}
             ::pcp/root                  3})))

  (testing "recursive nested input"
    (is (= (compute-run-graph
             (-> {::eql/query          [:names]
                  ::resolvers          '[{::pco/op-name nested-input-recursive
                                          ::pco/input   [:name {:children ...}]
                                          ::pco/output  [:names]}
                                         {::pco/op-name from-name
                                          ::pco/input   [:name]
                                          ::pco/output  [{:children [:name]}]}]
                  ::pcp/available-data {:name {}}}))
           '#::pcp{:nodes                 {1 {::pco/op-name      nested-input-recursive,
                                              ::pcp/node-id      1,
                                              ::pcp/expects      {:names {}},
                                              ::pcp/input        {:name     {},
                                                                  :children {}},
                                              ::pcp/node-parents #{2},},
                                           2 {::pco/op-name  from-name,
                                              ::pcp/node-id  2,
                                              ::pcp/expects  {:children {}},
                                              ::pcp/input    {:name {}},
                                              ::pcp/run-next 1}},
                   :index-ast             {:names    {:type         :prop,
                                                      :dispatch-key :names,
                                                      :key          :names},
                                           :children {:type         :join,
                                                      :key          :children,
                                                      :dispatch-key :children,
                                                      :query        ...}},
                   :index-resolver->nodes {nested-input-recursive #{1},
                                           from-name              #{2}},
                   :index-attrs           {:children #{2}, :names #{1}},
                   :root                  2})))

  (testing "optional nested input"
    (is (= (compute-run-graph
             (assoc
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
                    {::pco/input  [{:users [(pco/? :user/score)]}]
                     ::pco/output [:total-score]}
                    (fn [_ {:keys [users]}]
                      {:total-score (reduce + 0 (map :user/score users))}))])

               ::eql/query [:total-score]))
           '{::pcp/index-ast             {:total-score {:dispatch-key :total-score
                                                        :key          :total-score
                                                        :type         :prop}
                                          :users       {:children     [{:dispatch-key :user/score
                                                                        :key          :user/score
                                                                        :params       {::pco/optional? true}
                                                                        :type         :prop}]
                                                        :dispatch-key :users
                                                        :key          :users
                                                        :type         :join}}
             ::pcp/index-attrs           {:total-score #{1}
                                          :users       #{4}}
             ::pcp/index-resolver->nodes {total-score #{1}
                                          users       #{4}}
             ::pcp/nodes                 {1 {::pco/op-name      total-score
                                             ::pcp/expects      {:total-score {}}
                                             ::pcp/input        {:users {}}
                                             ::pcp/node-id      1
                                             ::pcp/node-parents #{4}}
                                          4 {::pco/op-name  users
                                             ::pcp/expects  {:users {:user/id {}}}
                                             ::pcp/input    {}
                                             ::pcp/node-id  4
                                             ::pcp/run-next 1}}
             ::pcp/root                  4}))

    (is (= (compute-run-graph
             (assoc
               (pci/register
                 [(pco/resolver 'users
                    {::pco/output [{:users [:user/id]}]})
                  (pco/resolver 'foo
                    {::pco/output [{:foo [:bar]}]}
                    (fn [_ _]
                      {}))
                  (pbir/static-attribute-map-resolver :user/id :user/score {})
                  (pco/resolver 'total-score
                    {::pco/input  [{:users [(pco/? :user/score)
                                            {:foo [(pco/? :bar)]}]}]
                     ::pco/output [:total-score]})])

               ::eql/query [:total-score]))
           '{::pcp/index-ast             {:total-score {:dispatch-key :total-score
                                                        :key          :total-score
                                                        :type         :prop}
                                          :users       {:children     [{:children     [{:dispatch-key :bar
                                                                                        :key          :bar
                                                                                        :params       {::pco/optional? true}
                                                                                        :type         :prop}]
                                                                        :dispatch-key :foo
                                                                        :key          :foo
                                                                        :type         :join}
                                                                       {:dispatch-key :user/score
                                                                        :key          :user/score
                                                                        :params       {::pco/optional? true}
                                                                        :type         :prop}]
                                                        :dispatch-key :users
                                                        :key          :users
                                                        :type         :join}}
             ::pcp/index-attrs           {:total-score #{1}
                                          :users       #{8}}
             ::pcp/index-resolver->nodes {total-score #{1}
                                          users       #{8}}
             ::pcp/nodes                 {1 {::pco/op-name      total-score
                                             ::pcp/expects      {:total-score {}}
                                             ::pcp/input        {:users {:foo {}}}
                                             ::pcp/node-id      1
                                             ::pcp/node-parents #{8}}
                                          8 {::pco/op-name  users
                                             ::pcp/expects  {:users {:user/id {}}}
                                             ::pcp/input    {}
                                             ::pcp/node-id  8
                                             ::pcp/run-next 1}}
             ::pcp/root                  8})))

  (testing "nested dependency on available data"
    (is (= (compute-run-graph
             (assoc
               (pci/register
                 [(pco/resolver 'ab->c
                    {::pco/input  [:a :b]
                     ::pco/output [:c]}
                    (fn [_ _]
                      {:c "val"}))
                  (pco/resolver 'items
                    {::pco/output [{:items [:a :b]}]}
                    (fn [_ _]
                      {:items [{:a 1 :b 2}]}))
                  (pco/resolver 'z
                    {::pco/input  [{:items [:c :b]}]
                     ::pco/output [:z]}
                    (fn [_ _]
                      {:z "Z"}))])

               ::eql/query [:z]))
           '{::pcp/index-ast             {:items {:children     [{:dispatch-key :c
                                                                  :key          :c
                                                                  :type         :prop}
                                                                 {:dispatch-key :b
                                                                  :key          :b
                                                                  :type         :prop}]
                                                  :dispatch-key :items
                                                  :key          :items
                                                  :type         :join}
                                          :z     {:dispatch-key :z
                                                  :key          :z
                                                  :type         :prop}}
             ::pcp/index-attrs           {:items #{3}
                                          :z     #{1}}
             ::pcp/index-resolver->nodes {items #{3}
                                          z     #{1}}
             ::pcp/nodes                 {1 {::pco/op-name      z
                                             ::pcp/expects      {:z {}}
                                             ::pcp/input        {:items {:b {}
                                                                         :c {}}}
                                             ::pcp/node-id      1
                                             ::pcp/node-parents #{3}}
                                          3 {::pco/op-name  items
                                             ::pcp/expects  {:items {:a {}
                                                                     :b {}}}
                                             ::pcp/input    {}
                                             ::pcp/node-id  3
                                             ::pcp/run-next 1}}
             ::pcp/root                  3})))

  (testing "nested plan failure"
    (is (thrown-with-msg?
          #?(:clj Throwable :cljs :default)
          #"Pathom can't find a path for the following elements in the query: \[:c] at path \[:a]"
          (compute-run-graph
            {::resolvers                    [{::pco/op-name 'nested-provider
                                              ::pco/output  [{:a [:b]}]}
                                             {::pco/op-name 'nested-requires
                                              ::pco/input   [{:a [:c]}]
                                              ::pco/output  [:d]}]

             :com.wsscode.pathom3.path/path []

             ::eql/query                    [:d]})))))

(deftest compute-run-graph-optional-inputs-test
  (testing "plan continues when optional thing is missing"
    (is (= (compute-run-graph
             (-> {::eql/query             [:foo]
                  ::resolvers             [{::pco/op-name 'foo
                                            ::pco/input   [:x (pco/? :y)]
                                            ::pco/output  [:foo]}
                                           {::pco/op-name 'x
                                            ::pco/output  [:x]}]
                  ::p.error/lenient-mode? true}))
           '#::pcp{:nodes                 {1 {::pco/op-name      foo,
                                              ::pcp/node-id      1,
                                              ::pcp/expects      {:foo {}},
                                              ::pcp/input        {:x {}},
                                              ::pcp/node-parents #{2},},
                                           2 {::pco/op-name  x,
                                              ::pcp/node-id  2,
                                              ::pcp/expects  {:x {}},
                                              ::pcp/input    {},
                                              ::pcp/run-next 1}},
                   :index-resolver->nodes {foo #{1}, x #{2}},
                   :unreachable-paths     {:y {}},
                   :index-ast             {:foo {:type         :prop,
                                                 :dispatch-key :foo,
                                                 :key          :foo}},
                   :index-attrs           {:foo #{1}, :x #{2}},
                   :root                  2})))

  (testing "adds optionals to plan, when available"
    (is (= (compute-run-graph
             (-> {::eql/query             [:foo]
                  ::resolvers             [{::pco/op-name 'foo
                                            ::pco/input   [:x (pco/? :y)]
                                            ::pco/output  [:foo]}
                                           {::pco/op-name 'x
                                            ::pco/output  [:x]}
                                           {::pco/op-name 'y
                                            ::pco/output  [:y]}]
                  ::p.error/lenient-mode? true}))
           '{::pcp/index-ast             {:foo {:dispatch-key :foo
                                                :key          :foo
                                                :type         :prop}}
             ::pcp/index-attrs           {:foo #{1}
                                          :x   #{2}
                                          :y   #{3}}
             ::pcp/index-resolver->nodes {foo #{1}
                                          x   #{2}
                                          y   #{3}}
             ::pcp/nodes                 {1 {::pco/op-name      foo
                                             ::pcp/expects      {:foo {}}
                                             ::pcp/input        {:x {}}
                                             ::pcp/node-id      1
                                             ::pcp/node-parents #{4}}
                                          2 {::pco/op-name      x
                                             ::pcp/expects      {:x {}}
                                             ::pcp/input        {}
                                             ::pcp/node-id      2
                                             ::pcp/node-parents #{4}}
                                          3 {::pco/op-name      y
                                             ::pcp/expects      {:y {}}
                                             ::pcp/input        {}
                                             ::pcp/node-id      3
                                             ::pcp/node-parents #{4}
                                             ::pcp/params       {::pco/optional? true}}
                                          4 {::pcp/node-id  4
                                             ::pcp/run-and  #{2
                                                              3}
                                             ::pcp/run-next 1}}
             ::pcp/root                  4})))

  (testing "only optional"
    (testing "unavailable"
      (is (= (compute-run-graph
               (-> {::eql/query             [:foo]
                    ::resolvers             [{::pco/op-name 'foo
                                              ::pco/input   [(pco/? :y)]
                                              ::pco/output  [:foo]}]
                    ::p.error/lenient-mode? true}))
             '#::pcp{:nodes                 {1 {::pco/op-name foo,
                                                ::pcp/node-id 1,
                                                ::pcp/expects {:foo {}},
                                                ::pcp/input   {},}},
                     :index-resolver->nodes {foo #{1}},
                     :unreachable-paths     {:y {}},
                     :index-ast             {:foo {:type         :prop,
                                                   :dispatch-key :foo,
                                                   :key          :foo}},
                     :root                  1,
                     :index-attrs           {:foo #{1}}})))

    (testing "available"
      (is (= (compute-run-graph
               (-> {::eql/query [:foo]
                    ::resolvers [{::pco/op-name 'foo
                                  ::pco/input   [(pco/? :y)]
                                  ::pco/output  [:foo]}
                                 {::pco/op-name 'y
                                  ::pco/output  [:y]}]}))
             '{::pcp/index-ast             {:foo {:dispatch-key :foo
                                                  :key          :foo
                                                  :type         :prop}}
               ::pcp/index-attrs           {:foo #{1}
                                            :y   #{2}}
               ::pcp/index-resolver->nodes {foo #{1}
                                            y   #{2}}
               ::pcp/nodes                 {1 {::pco/op-name      foo
                                               ::pcp/expects      {:foo {}}
                                               ::pcp/input        {}
                                               ::pcp/node-id      1
                                               ::pcp/node-parents #{2}}
                                            2 {::pco/op-name  y
                                               ::pcp/expects  {:y {}}
                                               ::pcp/input    {}
                                               ::pcp/node-id  2
                                               ::pcp/params   {::pco/optional? true}
                                               ::pcp/run-next 1}}
               ::pcp/root                  2})))))

(deftest compute-run-graph-placeholders-test
  (testing "just placeholder"
    (is (= (compute-run-graph
             {::pci/index-oir '{:a {{} #{a}}}
              ::eql/query     [{:>/p1 [:a]}]})
           '#::pcp{:nodes                 {1 {::pco/op-name a,
                                              ::pcp/node-id 1,
                                              ::pcp/expects {:a {}},
                                              ::pcp/input   {},}},
                   :index-ast             #:>{:p1 {:type         :join,
                                                   :dispatch-key :>/p1,
                                                   :key          :>/p1,
                                                   :query        [:a],
                                                   :children     [{:type         :prop,
                                                                   :dispatch-key :a,
                                                                   :key          :a}]}},
                   :placeholders          #{:>/p1},
                   :index-resolver->nodes {a #{1}},
                   :root                  1,
                   :index-attrs           {:a #{1}}})))


  (testing "placeholder + external"
    (is (= (compute-run-graph
             {::pci/index-oir '{:a {{} #{a}}
                                :b {{} #{b}}}
              ::eql/query     [:a
                               {:>/p1 [:b]}]})
           '#::pcp{:nodes                 {1 {::pco/op-name      a,
                                              ::pcp/node-id      1,
                                              ::pcp/expects      {:a {}},
                                              ::pcp/input        {},
                                              ::pcp/node-parents #{3}},
                                           2 {::pco/op-name      b,
                                              ::pcp/node-id      2,
                                              ::pcp/expects      {:b {}},
                                              ::pcp/input        {},
                                              ::pcp/node-parents #{3}},
                                           3 #::pcp{:node-id 3,
                                                    :run-and #{1
                                                               2}}},
                   :index-ast             {:a    {:type         :prop,
                                                  :dispatch-key :a,
                                                  :key          :a},
                                           :>/p1 {:type         :join,
                                                  :dispatch-key :>/p1,
                                                  :key          :>/p1,
                                                  :query        [:b],
                                                  :children     [{:type         :prop,
                                                                  :dispatch-key :b,
                                                                  :key          :b}]}},
                   :index-resolver->nodes {a #{1}, b #{2}},
                   :index-attrs           {:b #{2}, :a #{1}},
                   :placeholders          #{:>/p1},
                   :root                  3})))

  (testing "multiple placeholders repeating"
    (is (= (compute-run-graph
             {::pci/index-oir '{:a {{} #{a}}}
              ::eql/query     [{:>/p1 [:a]}
                               {:>/p2 [:a]}]})
           '#::pcp{:index-ast             #:>{:p1 {:children     [{:dispatch-key :a
                                                                   :key          :a
                                                                   :type         :prop}]
                                                   :dispatch-key :>/p1
                                                   :key          :>/p1
                                                   :query        [:a]
                                                   :type         :join}
                                              :p2 {:children     [{:dispatch-key :a
                                                                   :key          :a
                                                                   :type         :prop}]
                                                   :dispatch-key :>/p2
                                                   :key          :>/p2
                                                   :query        [:a]
                                                   :type         :join}}
                   :index-attrs           {:a #{1}}
                   :index-resolver->nodes {a #{1}}
                   :nodes                 {1 {::pco/op-name a
                                              ::pcp/expects {:a {}}
                                              ::pcp/input   {}
                                              ::pcp/node-id 1}}
                   :placeholders          #{:>/p1
                                            :>/p2}
                   :root                  1})))

  (testing "nested placeholders"
    (is (= (compute-run-graph
             {::pci/index-oir '{:a {{} #{a}}
                                :b {{} #{b}}}
              ::eql/query     [{:>/p1
                                [:a
                                 {:>/p2 [:b]}]}]})
           '#::pcp{:nodes                 {1 {::pco/op-name      a,
                                              ::pcp/node-id      1,
                                              ::pcp/expects      {:a {}},
                                              ::pcp/input        {},
                                              ::pcp/node-parents #{3}},
                                           2 {::pco/op-name      b,
                                              ::pcp/node-id      2,
                                              ::pcp/expects      {:b {}},
                                              ::pcp/input        {},
                                              ::pcp/node-parents #{3}},
                                           3 #::pcp{:node-id 3,,
                                                    :run-and #{1
                                                               2}}},
                   :index-ast             #:>{:p1 {:type         :join,
                                                   :dispatch-key :>/p1,
                                                   :key          :>/p1,
                                                   :query        [:a #:>{:p2 [:b]}],
                                                   :children     [{:type         :prop,
                                                                   :dispatch-key :a,
                                                                   :key          :a}
                                                                  {:type         :join,
                                                                   :dispatch-key :>/p2,
                                                                   :key          :>/p2,
                                                                   :query        [:b],
                                                                   :children     [{:type         :prop,
                                                                                   :dispatch-key :b,
                                                                                   :key          :b}]}]}},
                   :placeholders          #{:>/p1 :>/p2},
                   :index-resolver->nodes {a #{1}, b #{2}},
                   :index-attrs           {:b #{2}, :a #{1}},
                   :root                  3})))

  #_(testing "conflict between params"
      (is (= (compute-run-graph
               {::pci/index-oir '{:a {#{} #{a}}}
                ::eql/query     '[(:a {:foo "bar"})
                                  {:>/p1 [(:a {:foo "baz"})]}]})
             {}))))

(deftest compute-run-graph-params-test
  (testing "add params to resolver call"
    (is (= (compute-run-graph
             {::resolvers [{::pco/op-name 'a
                            ::pco/output  [:a]}]
              ::eql/query [(list :a {:x "y"})]})
           '{::pcp/nodes                 {1 {::pco/op-name a
                                             ::pcp/params  {:x "y"}
                                             ::pcp/node-id 1
                                             ::pcp/expects {:a {}}
                                             ::pcp/input   {}}}
             ::pcp/index-resolver->nodes {a #{1}}
             ::pcp/root                  1
             ::pcp/index-attrs           {:a #{1}}
             ::pcp/index-ast             {:a {:type         :prop,
                                              :dispatch-key :a,
                                              :key          :a,
                                              :params       {:x "y"}}}})))

  (testing "params while collapsing"
    (testing "params come from first node"
      (is (= (compute-run-graph
               {::pci/index-oir '{:a {{} #{a}}
                                  :b {{} #{a}}}
                ::eql/query     [(list :a {:x 1}) :b]})
             '#::pcp{:index-ast             {:a {:dispatch-key :a
                                                 :key          :a
                                                 :params       {:x 1}
                                                 :type         :prop}
                                             :b {:dispatch-key :b
                                                 :key          :b
                                                 :type         :prop}}
                     :index-attrs           {:a #{1}
                                             :b #{1}}
                     :index-resolver->nodes {a #{1}}
                     :nodes                 {1 {::pco/op-name a
                                                ::pcp/expects {:a {}
                                                               :b {}}
                                                ::pcp/input   {}
                                                ::pcp/node-id 1
                                                ::pcp/params  {:x 1}}}
                     :root                  1})))))

(deftest compute-run-graph-optimize-test
  (testing "optimize AND nodes"
    (is (= (compute-run-graph
             {::pci/index-oir {:a {{} #{'x}}
                               :b {{} #{'x}}}
              ::eql/query     [:a :b]})
           '#::pcp{:nodes                 {1 {::pco/op-name x,
                                              ::pcp/expects {:a {},
                                                             :b {}},
                                              ::pcp/input   {},
                                              ::pcp/node-id 1}},
                   :index-ast             {:a {:type         :prop,
                                               :dispatch-key :a,
                                               :key          :a},
                                           :b {:type         :prop,
                                               :dispatch-key :b,
                                               :key          :b}},
                   :index-resolver->nodes {x #{1}},
                   :index-attrs           {:a #{1}, :b #{1}},
                   :root                  1}))

    (testing "multiple nodes"
      (is (= (compute-run-graph
               {::pci/index-oir {:a {{} #{'x}}
                                 :b {{} #{'x}}
                                 :c {{} #{'x}}}
                ::eql/query     [:a :b :c]})
             '#::pcp{:nodes                 {1 {::pco/op-name x,
                                                ::pcp/expects {:a {},
                                                               :b {},
                                                               :c {}},
                                                ::pcp/input   {},
                                                ::pcp/node-id 1}},
                     :index-ast             {:a {:type         :prop,
                                                 :dispatch-key :a,
                                                 :key          :a},
                                             :b {:type         :prop,
                                                 :dispatch-key :b,
                                                 :key          :b},
                                             :c {:type         :prop,
                                                 :dispatch-key :c,
                                                 :key          :c}},
                     :index-resolver->nodes {x #{1}},
                     :index-attrs           {:a #{1}, :b #{1}, :c #{1}},
                     :root                  1})))

    (testing "multiple resolvers"
      (is (= (compute-run-graph
               {::pci/index-oir {:a {{} #{'x}}
                                 :b {{} #{'y}}
                                 :c {{} #{'x}}
                                 :d {{} #{'z}}
                                 :e {{} #{'z}}
                                 :f {{} #{'x}}}
                ::eql/query     [:a :b :c :d :e :f]})
             '#::pcp{:nodes                 {1 {::pco/op-name      x,
                                                ::pcp/expects      {:a {},
                                                                    :c {},
                                                                    :f {}},
                                                ::pcp/input        {},
                                                ::pcp/node-id      1,
                                                ::pcp/node-parents #{7}},
                                             2 {::pco/op-name      y,
                                                ::pcp/expects      {:b {}},
                                                ::pcp/input        {},
                                                ::pcp/node-id      2,
                                                ::pcp/node-parents #{7}},
                                             4 {::pco/op-name      z,
                                                ::pcp/expects      {:d {},
                                                                    :e {}},
                                                ::pcp/input        {},
                                                ::pcp/node-id      4,
                                                ::pcp/node-parents #{7}},
                                             7 #::pcp{:node-id 7,
                                                      :run-and #{1
                                                                 4
                                                                 2}}},
                     :index-ast             {:a {:type         :prop,
                                                 :dispatch-key :a,
                                                 :key          :a},
                                             :b {:type         :prop,
                                                 :dispatch-key :b,
                                                 :key          :b},
                                             :c {:type         :prop,
                                                 :dispatch-key :c,
                                                 :key          :c},
                                             :d {:type         :prop,
                                                 :dispatch-key :d,
                                                 :key          :d},
                                             :e {:type         :prop,
                                                 :dispatch-key :e,
                                                 :key          :e},
                                             :f {:type         :prop,
                                                 :dispatch-key :f,
                                                 :key          :f}},
                     :index-resolver->nodes {x #{1}, y #{2}, z #{4}},
                     :index-attrs           {:a #{1},
                                             :b #{2},
                                             :c #{1},
                                             :d #{4},
                                             :e #{4},
                                             :f #{1}},
                     :root                  7}))))

  (is (= (compute-run-graph
           {::pci/index-oir {:a {{} #{'x}}
                             :b {{} #{'x}}
                             :c {{:a {} :b {}} #{'c}}}
            ::eql/query     [:c]})
         '{::pcp/nodes                 {1 {::pco/op-name      c,
                                           ::pcp/expects      {:c {}},
                                           ::pcp/input        {:a {},
                                                               :b {}},
                                           ::pcp/node-id      1,
                                           ::pcp/node-parents #{2}},
                                        2 {::pco/op-name  x,
                                           ::pcp/expects  {:a {},
                                                           :b {}},
                                           ::pcp/input    {},
                                           ::pcp/node-id  2,
                                           ::pcp/run-next 1}},
           ::pcp/index-ast             {:c {:type         :prop,
                                            :dispatch-key :c,
                                            :key          :c}},
           ::pcp/index-resolver->nodes {c #{1}, x #{2}},
           ::pcp/index-attrs           {:c #{1}, :a #{2}, :b #{2}},
           ::pcp/root                  2}))

  (is (= (compute-run-graph
           {::pci/index-oir {:a {{:z {}} #{'x}}
                             :b {{:z {}} #{'x}}
                             :z {{} #{'z}}
                             :c {{:a {} :b {}} #{'c}}}
            ::eql/query     [:c]})
         '{::pcp/nodes                 {1 {::pco/op-name      c,
                                           ::pcp/expects      {:c {}},
                                           ::pcp/input        {:a {},
                                                               :b {}},
                                           ::pcp/node-id      1,
                                           ::pcp/node-parents #{2}},
                                        2 {::pco/op-name      x,
                                           ::pcp/expects      {:a {},
                                                               :b {}},
                                           ::pcp/input        {:z {}},
                                           ::pcp/node-id      2,
                                           ::pcp/node-parents #{3},
                                           ::pcp/run-next     1},
                                        3 {::pco/op-name  z,
                                           ::pcp/expects  {:z {}},
                                           ::pcp/input    {},
                                           ::pcp/node-id  3,
                                           ::pcp/run-next 2}},
           ::pcp/index-ast             {:c {:type         :prop,
                                            :dispatch-key :c,
                                            :key          :c}},
           ::pcp/index-resolver->nodes {c #{1}, x #{2}, z #{3}},
           ::pcp/index-attrs           {:c #{1}, :a #{2}, :z #{3}, :b #{2}},
           ::pcp/root                  3}))

  (testing "optimize AND not at root"
    (is (= (compute-run-graph
             {::pci/index-oir {:a {{:z {}} #{'x}}
                               :b {{:z {}} #{'x}}
                               :z {{} #{'z}}}
              ::eql/query     [:a :b]})
           '#::pcp{:nodes                 {1 {::pco/op-name      x,
                                              ::pcp/expects      {:a {},
                                                                  :b {}},
                                              ::pcp/input        {:z {}},
                                              ::pcp/node-id      1,
                                              ::pcp/node-parents #{2}},
                                           2 {::pco/op-name  z,
                                              ::pcp/expects  {:z {}},
                                              ::pcp/input    {},
                                              ::pcp/node-id  2,
                                              ::pcp/run-next 1}},
                   :index-ast             {:a {:type         :prop,
                                               :dispatch-key :a,
                                               :key          :a},
                                           :b {:type         :prop,
                                               :dispatch-key :b,
                                               :key          :b}},
                   :index-resolver->nodes {x #{1}, z #{2}},
                   :index-attrs           {:a #{1}, :z #{2}, :b #{1}},
                   :root                  2})))

  (testing "optimize AND next node")

  (testing "OR nodes"
    (testing "navigate OR nodes"
      (is (= (compute-run-graph
               {::pci/index-oir '{:a {{:x {}}       #{ax}
                                      {:y {} :z {}} #{ayz}}
                                  :x {{} #{x}}
                                  :y {{} #{yz}}
                                  :z {{} #{yz}}}
                ;::pcp/snapshots* (atom [])
                ::eql/query     [:a]})
             '{::pcp/nodes                 {1 {::pco/op-name      ax,
                                               ::pcp/expects      {:a {}},
                                               ::pcp/input        {:x {}},
                                               ::pcp/node-id      1,
                                               ::pcp/node-parents #{2}},
                                            2 {::pco/op-name      x,
                                               ::pcp/expects      {:x {}},
                                               ::pcp/input        {},
                                               ::pcp/node-id      2,
                                               ::pcp/run-next     1,
                                               ::pcp/node-parents #{7}},
                                            3 {::pco/op-name      ayz,
                                               ::pcp/expects      {:a {}},
                                               ::pcp/input        {:y {},
                                                                   :z {}},
                                               ::pcp/node-id      3,
                                               ::pcp/node-parents #{4}},
                                            4 {::pco/op-name      yz,
                                               ::pcp/expects      {:y {},
                                                                   :z {}},
                                               ::pcp/input        {},
                                               ::pcp/node-id      4,
                                               ::pcp/node-parents #{7},
                                               ::pcp/run-next     3},
                                            7 {::pcp/expects {:a {}},
                                               ::pcp/node-id 7,
                                               ::pcp/run-or  #{4
                                                               2}}},
               ::pcp/index-ast             {:a {:type         :prop,
                                                :dispatch-key :a,
                                                :key          :a}},
               ::pcp/index-resolver->nodes {ax  #{1},
                                            x   #{2},
                                            ayz #{3},
                                            yz  #{4}},
               ::pcp/index-attrs           {:a #{1 3}, :x #{2}, :y #{4}, :z #{4}},
               ::pcp/root                  7})))

    #_(testing "merge equal OR's on AND's"
        (is (= (compute-run-graph
                 {::pci/index-oir  {:a {{} #{'x 'x2}}
                                    :b {{} #{'x 'x2}}}
                  ::pcp/snapshots* (atom [])
                  ::eql/query      [:a :b]})
               '{})))))

(deftest compute-run-graph-dynamic-resolvers-test
  (testing "unreachable"
    (is (= (compute-run-graph
             {::pci/index-resolvers   {'dynamic-resolver {::pco/op-name           'dynamic-resolver
                                                          ::pco/cache?            false
                                                          ::pco/dynamic-resolver? true
                                                          ::pco/resolve           (fn [_ _])}}
              ::pci/index-oir         {:release/script {{:db/id {}} #{'dynamic-resolver}}}
              ::eql/query             [:release/script]
              ::p.error/lenient-mode? true})
           {::pcp/index-ast            {:release/script {:dispatch-key :release/script
                                                         :key          :release/script
                                                         :type         :prop}}
            ::pcp/nodes                {}
            ::pcp/unreachable-paths    {:db/id          {}
                                        :release/script {}}
            ::pcp/verification-failed? true})))

  (testing "simple dynamic call"
    (is (= (compute-run-graph
             {::pci/index-resolvers {'dynamic-resolver
                                     {::pco/op-name           'dynamic-resolver
                                      ::pco/cache?            false
                                      ::pco/dynamic-resolver? true
                                      ::pco/resolve           (fn [_ _])}}
              ::pci/index-oir       {:release/script {{:db/id {}} #{'dynamic-resolver}}}
              ::pcp/available-data  {:db/id {}}
              ::eql/query           [:release/script]})

           {::pcp/nodes                 {1 {::pco/op-name        'dynamic-resolver
                                            ::pcp/source-op-name 'dynamic-resolver
                                            ::pcp/node-id        1
                                            ::pcp/expects        {:release/script {}}
                                            ::pcp/input          {:db/id {}}
                                            ::pcp/foreign-ast    (eql/query->ast [:release/script])}}
            ::pcp/index-resolver->nodes {'dynamic-resolver #{1}}
            ::pcp/root                  1
            ::pcp/index-attrs           {:release/script #{1}}
            ::pcp/index-ast             {:release/script {:type         :prop,
                                                          :dispatch-key :release/script,
                                                          :key          :release/script}}}))

    (testing "dynamic extensions"
      (is (= (compute-run-graph
               {::pci/index-resolvers {'dynamic-resolver
                                       {::pco/op-name           'dynamic-resolver
                                        ::pco/cache?            false
                                        ::pco/dynamic-resolver? true
                                        ::pco/resolve           (fn [_ _])}

                                       'a
                                       {::pco/op-name      'a
                                        ::pco/input        []
                                        ::pco/output       [:a]
                                        ::pco/requires     {}
                                        ::pco/provides     {:a {}}
                                        ::pco/dynamic-name 'dynamic-resolver}}
                ::pci/index-oir       {:a {{} #{'a}}}
                ::pcp/available-data  {}
                ::eql/query           [:a]})

             '#::pcp{:nodes                 {1 {::pco/op-name        dynamic-resolver
                                                ::pcp/source-op-name a
                                                ::pcp/expects        {:a {}}
                                                ::pcp/input          {}
                                                ::pcp/node-id        1
                                                ::pcp/foreign-ast    {:type     :root
                                                                      :children [{:type         :prop
                                                                                  :dispatch-key :a
                                                                                  :key          :a}]}}}
                     :index-ast             {:a {:type         :prop
                                                 :dispatch-key :a
                                                 :key          :a}}
                     :index-resolver->nodes {dynamic-resolver #{1}}
                     :index-attrs           {:a #{1}}
                     :root                  1})))

    (testing "retain params"
      (is (= (compute-run-graph
               {::dynamics  {'dyn [{::pco/op-name 'a
                                    ::pco/output  [:a]}]}
                ::eql/query [(list :a {:b "c"})]})

             '{::pcp/nodes                 {1 {::pco/op-name        dyn,
                                               ::pcp/expects        {:a {}},
                                               ::pcp/input          {},
                                               ::pcp/node-id        1,
                                               ::pcp/params         {:b "c"},
                                               ::pcp/source-op-name a,
                                               ::pcp/foreign-ast    {:type     :root,
                                                                     :children [{:type         :prop,
                                                                                 :dispatch-key :a,
                                                                                 :key          :a,
                                                                                 :params       {:b "c"}}]}}},
               ::pcp/index-ast             {:a {:type         :prop,
                                                :dispatch-key :a,
                                                :key          :a,
                                                :params       {:b "c"}}},
               ::pcp/index-resolver->nodes {dyn #{1}},
               ::pcp/index-attrs           {:a #{1}},
               ::pcp/root                  1}))

      (testing "nested params"
        (is (= (compute-run-graph
                 {::dynamics  {'dyn [{::pco/op-name 'a
                                      ::pco/output  [{:a [:b]}]}]}
                  ::eql/query [{:a [(list :b {:p "v"})]}]})
               '{::pcp/nodes                 {1 {::pco/op-name        dyn,
                                                 ::pcp/expects        {:a {:b {}}},
                                                 ::pcp/input          {},
                                                 ::pcp/node-id        1,
                                                 ::pcp/source-op-name a,
                                                 ::pcp/foreign-ast    {:type     :root,
                                                                       :children [{:type         :join,
                                                                                   :dispatch-key :a,
                                                                                   :key          :a,
                                                                                   :query        [(:b {:p "v"})],
                                                                                   :children     [{:type         :prop,
                                                                                                   :key          :b,
                                                                                                   :dispatch-key :b
                                                                                                   :params       {:p "v"}}]}]}}},
                 ::pcp/index-ast             {:a {:type         :join,
                                                  :dispatch-key :a,
                                                  :key          :a,
                                                  :query        [(:b {:p "v"})],
                                                  :children     [{:type         :prop,
                                                                  :dispatch-key :b,
                                                                  :key          :b,
                                                                  :params       {:p "v"}}]}},
                 ::pcp/index-resolver->nodes {dyn #{1}},
                 ::pcp/index-attrs           {:a #{1}},
                 ::pcp/root                  1}))

        (is (= (compute-run-graph
                 {::dynamics  {'dyn [{::pco/op-name 'a
                                      ::pco/output  [{:a [:b]}]}
                                     {::pco/op-name 'c
                                      ::pco/input   [:b]
                                      ::pco/output  [:c]}]}
                  ::eql/query [{:a [(list :c {:p "v"})]}]})
               '{::pcp/nodes                 {2 {::pco/op-name        dyn,
                                                 ::pcp/expects        {:a {:c {}}},
                                                 ::pcp/input          {},
                                                 ::pcp/node-id        2,
                                                 ::pcp/source-op-name a,
                                                 ::pcp/foreign-ast    {:type     :root,
                                                                       :children [{:type         :join,
                                                                                   :dispatch-key :a,
                                                                                   :key          :a,
                                                                                   :query        [(:c {:p "v"})],
                                                                                   :children     [{:type         :prop,
                                                                                                   :key          :c,
                                                                                                   :dispatch-key :c
                                                                                                   :params       {:p "v"}}]}]}}},
                 ::pcp/index-ast             {:a {:type         :join,
                                                  :dispatch-key :a,
                                                  :key          :a,
                                                  :query        [(:c {:p "v"})],
                                                  :children     [{:type         :prop,
                                                                  :dispatch-key :c,
                                                                  :key          :c,
                                                                  :params       {:p "v"}}]}},
                 ::pcp/index-resolver->nodes {dyn #{2}},
                 ::pcp/index-attrs           {:a #{2}},
                 ::pcp/root                  2})))))

  (testing "optimize multiple calls"
    (is (= (compute-run-graph
             (-> {::pci/index-resolvers {'dynamic-resolver
                                         {::pco/op-name           'dynamic-resolver
                                          ::pco/cache?            false
                                          ::pco/dynamic-resolver? true
                                          ::pco/resolve           (fn [_ _])}}
                  ::pci/index-oir       {:a {{} #{'dynamic-resolver}}
                                         :b {{} #{'dynamic-resolver}}}
                  ::eql/query           [:a :b]}))

           {::pcp/nodes                 {1 {::pco/op-name     'dynamic-resolver
                                            ::pcp/node-id     1
                                            ::pcp/expects     {:a {} :b {}}
                                            ::pcp/input       {}
                                            ::pcp/foreign-ast (eql/query->ast [:a :b])}}
            ::pcp/index-resolver->nodes {'dynamic-resolver #{1}}
            ::pcp/index-attrs           {:a #{1}, :b #{1}}
            ::pcp/index-ast             {:a {:type         :prop,
                                             :dispatch-key :a,
                                             :key          :a},
                                         :b {:type         :prop,
                                             :dispatch-key :b,
                                             :key          :b}}
            ::pcp/root                  1})))

  (testing "optimized with dependencies"
    (is (= (compute-run-graph
             (-> {::pci/index-resolvers {'dynamic-resolver
                                         {::pco/op-name           'dynamic-resolver
                                          ::pco/cache?            false
                                          ::pco/dynamic-resolver? true
                                          ::pco/resolve           (fn [_ _])}}
                  ::pci/index-oir       {:release/script {{:db/id {}} #{'dynamic-resolver}}
                                         :label/type     {{:db/id {}} #{'dynamic-resolver}}}
                  ::eql/query           [:release/script :label/type]
                  ::resolvers           [{::pco/op-name 'id
                                          ::pco/output  [:db/id]}]}))

           '#::pcp{:nodes                 {1 {::pco/op-name      dynamic-resolver,
                                              ::pcp/expects      {:release/script {},
                                                                  :label/type     {}},
                                              ::pcp/input        #:db{:id {}},
                                              ::pcp/node-id      1,
                                              ::pcp/foreign-ast  {:type     :root,
                                                                  :children [{:type         :prop,
                                                                              :dispatch-key :release/script,
                                                                              :key          :release/script}
                                                                             {:type         :prop,
                                                                              :dispatch-key :label/type,
                                                                              :key          :label/type}]},
                                              ::pcp/node-parents #{2}},
                                           2 {::pco/op-name  id,
                                              ::pcp/expects  #:db{:id {}},
                                              ::pcp/input    {},
                                              ::pcp/node-id  2,
                                              ::pcp/run-next 1}},
                   :index-ast             {:release/script {:type         :prop,
                                                            :dispatch-key :release/script,
                                                            :key          :release/script},
                                           :label/type     {:type         :prop,
                                                            :dispatch-key :label/type,
                                                            :key          :label/type}},
                   :index-resolver->nodes {dynamic-resolver #{1},
                                           id               #{2}},
                   :index-attrs           {:release/script #{1},
                                           :db/id          #{2},
                                           :label/type     #{1}},
                   :root                  2})))

  (testing "dependency reuse"
    (is (= (compute-run-graph
             (->
               {::eql/query          [:d]
                ::pcp/available-data {:a {}}}
               (pci/register
                 [(pco/resolver 'dynamic-resolver
                    {::pco/dynamic-resolver? true})
                  (pco/resolver 'b
                    {::pco/input        [:a]
                     ::pco/output       [:b]
                     ::pco/dynamic-name 'dynamic-resolver})
                  (pco/resolver 'c
                    {::pco/input        [:b]
                     ::pco/output       [:c]
                     ::pco/dynamic-name 'dynamic-resolver})
                  (pco/resolver 'other-resolver
                    {::pco/input  [:b :c]
                     ::pco/output [:d]})])))
           '{::pcp/nodes                 {1 {::pco/op-name      other-resolver,
                                             ::pcp/expects      {:d {}},
                                             ::pcp/input        {:b {},
                                                                 :c {}},
                                             ::pcp/node-id      1,
                                             ::pcp/node-parents #{2}},
                                          2 {::pco/op-name     dynamic-resolver,
                                             ::pcp/expects     {:b {},
                                                                :c {}},
                                             ::pcp/input       {:a {}},
                                             ::pcp/node-id     2,
                                             ::pcp/foreign-ast {:type     :root,
                                                                :children [{:type         :prop,
                                                                            :key          :b,
                                                                            :dispatch-key :b}
                                                                           {:type         :prop,
                                                                            :key          :c,
                                                                            :dispatch-key :c}]},
                                             ::pcp/run-next    1}},
             ::pcp/index-ast             {:d {:type         :prop,
                                              :dispatch-key :d,
                                              :key          :d}},
             ::pcp/index-resolver->nodes {other-resolver   #{1},
                                          dynamic-resolver #{2}},
             ::pcp/index-attrs           {:d #{1}, :b #{2}, :c #{2}},
             ::pcp/root                  2})))

  (testing "optimized with dependencies"
    (is (= (compute-run-graph
             (-> {::pci/index-resolvers {'dynamic-resolver
                                         {::pco/op-name           'dynamic-resolver
                                          ::pco/cache?            false
                                          ::pco/dynamic-resolver? true
                                          ::pco/resolve           (fn [_ _])}}
                  ::pci/index-oir       {:release/script {{:db/id {}} #{'dynamic-resolver}}
                                         :label/type     {{:db/id {}} #{'dynamic-resolver}}}
                  ::eql/query           [:release/script :label/type]
                  ::resolvers           [{::pco/op-name 'id
                                          ::pco/output  [:db/id]}]}))

           '{::pcp/nodes                 {1 {::pco/op-name      dynamic-resolver,
                                             ::pcp/expects      {:release/script {},
                                                                 :label/type     {}},
                                             ::pcp/input        {:db/id {}},
                                             ::pcp/node-id      1,
                                             ::pcp/foreign-ast  {:type     :root,
                                                                 :children [{:type         :prop,
                                                                             :dispatch-key :release/script,
                                                                             :key          :release/script}
                                                                            {:type         :prop,
                                                                             :dispatch-key :label/type,
                                                                             :key          :label/type}]},
                                             ::pcp/node-parents #{2}},
                                          2 {::pco/op-name  id,
                                             ::pcp/expects  {:db/id {}},
                                             ::pcp/input    {},
                                             ::pcp/node-id  2,
                                             ::pcp/run-next 1}},
             ::pcp/index-ast             {:release/script {:type         :prop,
                                                           :dispatch-key :release/script,
                                                           :key          :release/script},
                                          :label/type     {:type         :prop,
                                                           :dispatch-key :label/type,
                                                           :key          :label/type}},
             ::pcp/index-resolver->nodes {dynamic-resolver #{1},
                                          id               #{2}},
             ::pcp/index-attrs           {:release/script #{1},
                                          :db/id          #{2},
                                          :label/type     #{1}},
             ::pcp/root                  2})))

  (testing "chained calls"
    (is (= (compute-run-graph
             (-> {::pci/index-resolvers {'dynamic-resolver
                                         {::pco/op-name           'dynamic-resolver
                                          ::pco/cache?            false
                                          ::pco/dynamic-resolver? true
                                          ::pco/resolve           (fn [_ _])}}
                  ::pci/index-oir       {:a {{} #{'dynamic-resolver}}
                                         :b {{:a {}} #{'dynamic-resolver}}}
                  ::eql/query           [:b]}))

           '{::pcp/nodes                 {2 {::pco/op-name     dynamic-resolver,
                                             ::pcp/expects     {:b {}},
                                             ::pcp/input       {},
                                             ::pcp/node-id     2,
                                             ::pcp/foreign-ast {:type     :root,
                                                                :children [{:type         :prop,
                                                                            :dispatch-key :b,
                                                                            :key          :b}]}}},
             ::pcp/index-ast             {:b {:type         :prop,
                                              :dispatch-key :b,
                                              :key          :b}},
             ::pcp/index-resolver->nodes {dynamic-resolver #{2}},
             ::pcp/index-attrs           {:b #{2}},
             ::pcp/root                  2}))

    (is (= (compute-run-graph
             (-> {::pci/index-resolvers {'dynamic-resolver
                                         {::pco/op-name           'dynamic-resolver
                                          ::pco/cache?            false
                                          ::pco/dynamic-resolver? true
                                          ::pco/resolve           (fn [_ _])}}
                  ::pci/index-oir       {:a {{} #{'dynamic-resolver}}
                                         :b {{:a {}} #{'dynamic-resolver}}
                                         :c {{:b {}} #{'dynamic-resolver}}}
                  ::eql/query           [:c]}))

           '{::pcp/nodes                 {3 {::pco/op-name     dynamic-resolver,
                                             ::pcp/expects     {:c {}},
                                             ::pcp/input       {},
                                             ::pcp/node-id     3,
                                             ::pcp/foreign-ast {:type     :root,
                                                                :children [{:type         :prop,
                                                                            :dispatch-key :c,
                                                                            :key          :c}]}}},
             ::pcp/index-ast             {:c {:type         :prop,
                                              :dispatch-key :c,
                                              :key          :c}},
             ::pcp/index-resolver->nodes {dynamic-resolver #{3}},
             ::pcp/index-attrs           {:c #{3}},
             ::pcp/root                  3}))

    (is (= (compute-run-graph
             (-> {::pci/index-resolvers {'dynamic-resolver
                                         {::pco/op-name           'dynamic-resolver
                                          ::pco/cache?            false
                                          ::pco/dynamic-resolver? true
                                          ::pco/resolve           (fn [_ _])}}
                  ::resolvers           [{::pco/op-name 'z
                                          ::pco/output  [:z]}]
                  ::pci/index-oir       {:a {{:z {}} #{'dynamic-resolver}}
                                         :b {{:a {}} #{'dynamic-resolver}}}
                  ::eql/query           [:b]}))

           '{::pcp/nodes                 {2 {::pco/op-name      dynamic-resolver,
                                             ::pcp/expects      {:b {}},
                                             ::pcp/input        {:z {}},
                                             ::pcp/node-id      2,
                                             ::pcp/foreign-ast  {:type     :root,
                                                                 :children [{:type         :prop,
                                                                             :dispatch-key :b,
                                                                             :key          :b}]},
                                             ::pcp/node-parents #{3}},
                                          3 {::pco/op-name  z,
                                             ::pcp/expects  {:z {}},
                                             ::pcp/input    {},
                                             ::pcp/node-id  3,
                                             ::pcp/run-next 2}},
             ::pcp/index-ast             {:b {:type         :prop,
                                              :dispatch-key :b,
                                              :key          :b}},
             ::pcp/index-resolver->nodes {dynamic-resolver #{2},
                                          z                #{3}},
             ::pcp/index-attrs           {:b #{2}, :z #{3}},
             ::pcp/root                  3}))

    (testing "chain with dynamic at start"
      (is (= (compute-run-graph
               (-> {::pci/index-resolvers {'dynamic-resolver
                                           {::pco/op-name           'dynamic-resolver
                                            ::pco/cache?            false
                                            ::pco/dynamic-resolver? true
                                            ::pco/resolve           (fn [_ _])}}
                    ::resolvers           [{::pco/op-name 'z
                                            ::pco/input   [:b]
                                            ::pco/output  [:z]}]
                    ::pci/index-oir       {:a {{} #{'dynamic-resolver}}
                                           :b {{:a {}} #{'dynamic-resolver}}}
                    ::eql/query           [:z]}))

             '{::pcp/nodes                 {1 {::pco/op-name      z,
                                               ::pcp/expects      {:z {}},
                                               ::pcp/input        {:b {}},
                                               ::pcp/node-id      1,
                                               ::pcp/node-parents #{3}},
                                            3 {::pco/op-name     dynamic-resolver,
                                               ::pcp/expects     {:b {}},
                                               ::pcp/input       {},
                                               ::pcp/node-id     3,
                                               ::pcp/foreign-ast {:type     :root,
                                                                  :children [{:type         :prop,
                                                                              :key          :b,
                                                                              :dispatch-key :b}]},
                                               ::pcp/run-next    1}},
               ::pcp/index-ast             {:z {:type         :prop,
                                                :dispatch-key :z,
                                                :key          :z}},
               ::pcp/index-resolver->nodes {z                #{1},
                                            dynamic-resolver #{3}},
               ::pcp/index-attrs           {:z #{1}, :b #{3}},
               ::pcp/root                  3})))

    (testing "merging long chains"
      (is (= (compute-run-graph
               (-> {::dynamics  {'dyn [{::pco/op-name 'a
                                        ::pco/output  [:a]}
                                       {::pco/op-name 'b
                                        ::pco/input   [:a]
                                        ::pco/output  [:b]}
                                       {::pco/op-name 'c
                                        ::pco/input   [:b]
                                        ::pco/output  [:c]}
                                       {::pco/op-name 'd
                                        ::pco/input   [:c]
                                        ::pco/output  [:d]}
                                       {::pco/op-name 'e
                                        ::pco/input   [:d]
                                        ::pco/output  [:e]}]}
                    ::eql/query [:e]}))
             '{::pcp/nodes                 {5 {::pco/op-name     dyn,
                                               ::pcp/expects     {:e {}},
                                               ::pcp/input       {},
                                               ::pcp/node-id     5,
                                               ::pcp/foreign-ast {:type     :root,
                                                                  :children [{:type         :prop,
                                                                              :dispatch-key :e,
                                                                              :key          :e}]}}},
               ::pcp/index-ast             {:e {:type         :prop,
                                                :dispatch-key :e,
                                                :key          :e}},
               ::pcp/index-resolver->nodes {dyn #{5}},
               ::pcp/index-attrs           {:e #{5}},
               ::pcp/root                  5}))))

  ; not sure if I'm going though with this
  #_(testing "transient attributes"
      (is (= (debug-compute-run-graph
               (-> {::dynamics            {'dyn [{::pco/op-name 'type
                                                  ::pco/input   [:type]
                                                  ::pco/output  [:ta :tb :tc]}
                                                 {::pco/op-name 'type-by-id
                                                  ::pco/input   [:tid]
                                                  ::pco/output  [:type]}]}
                    ::pci/transient-attrs #{:type}
                    ::pcp/available-data  {:tid {}}
                    ::eql/query           [:tb :type]}))
             '{::pcp/nodes                 {2 {::pco/op-name     dyn,
                                               ::pcp/expects     {:tb {}},
                                               ::pcp/input       {:tid {}},
                                               ::pcp/node-id     2,
                                               ::pcp/foreign-ast {:type     :root,
                                                                  :children [{:type         :prop,
                                                                              :dispatch-key :tb,
                                                                              :key          :tb}]}}},
               ::pcp/index-ast             {:tb   {:type         :prop,
                                                   :dispatch-key :tb,
                                                   :key          :tb},
                                            :type {:type         :prop,
                                                   :dispatch-key :type,
                                                   :key          :type}},
               ::pcp/index-resolver->nodes {dyn #{2}},
               ::pcp/index-attrs           {:tb #{2}},
               ::pcp/root                  2}))))

(deftest compute-run-graph-dynamic-nested-queries-test
  (testing "user query request directly available nested data"
    (is (= (compute-run-graph
             {::pci/index-resolvers {'dyn {::pco/op-name           'dyn
                                           ::pco/cache?            false
                                           ::pco/dynamic-resolver? true
                                           ::pco/resolve           (fn [_ _])}
                                     'a   {::pco/op-name      'a
                                           ::pco/dynamic-name 'dyn
                                           ::pco/output       [{:a [:b :c]}]
                                           ::pco/provides     {:a {:b {}
                                                                   :c {}}}
                                           ::pco/resolve      (fn [_ _])}}
              ::pci/index-oir       {:a {{} #{'a}}}
              ::eql/query           [{:a [:b]}]})
           {::pcp/nodes                 {1 {::pco/op-name        'dyn
                                            ::pcp/node-id        1
                                            ::pcp/expects        {:a {:b {}}}
                                            ::pcp/input          {}
                                            ::pcp/source-op-name 'a
                                            ::pcp/foreign-ast    (eql/query->ast [{:a [:b]}])}}
            ::pcp/index-resolver->nodes '{dyn #{1}}
            ::pcp/root                  1
            ::pcp/index-attrs           {:a #{1}}
            ::pcp/index-ast             {:a {:type         :join,
                                             :dispatch-key :a,
                                             :key          :a,
                                             :query        [:b],
                                             :children     [{:type         :prop,
                                                             :dispatch-key :b,
                                                             :key          :b}]}}})))

  (testing "nested dependency on directly available data in the output"
    (is (= (compute-run-graph
             {::pci/index-resolvers {'dyn {::pco/op-name           'dyn
                                           ::pco/cache?            false
                                           ::pco/dynamic-resolver? true
                                           ::pco/resolve           (fn [_ _])}
                                     'a   {::pco/op-name      'a
                                           ::pco/dynamic-name 'dyn
                                           ::pco/output       [{:a [:b]}]
                                           ::pco/resolve      (fn [_ _])}}
              ::pci/index-oir       {:a {{} #{'a}}}
              ::resolvers           [{::pco/op-name 'c
                                      ::pco/input   [:b]
                                      ::pco/output  [:c]}]
              ::eql/query           [{:a [:c]}]})
           {::pcp/nodes                 {2 {::pco/op-name        'dyn
                                            ::pcp/source-op-name 'a
                                            ::pcp/node-id        2
                                            ::pcp/expects        {:a {:b {}}}
                                            ::pcp/input          {}
                                            ::pcp/foreign-ast    (eql/query->ast [{:a [:b]}])}}
            ::pcp/index-resolver->nodes '{dyn #{2}}
            ::pcp/root                  2
            ::pcp/index-attrs           {:a #{2}}
            ::pcp/index-ast             {:a {:type         :join,
                                             :dispatch-key :a,
                                             :key          :a,
                                             :query        [:c],
                                             :children     [{:type         :prop,
                                                             :dispatch-key :c,
                                                             :key          :c}]}}})))

  (testing "nested extended process"
    (is (= (compute-run-graph
             {::pci/index-resolvers {'dyn {::pco/op-name           'dyn
                                           ::pco/cache?            false
                                           ::pco/dynamic-resolver? true
                                           ::pco/resolve           (fn [_ _])}
                                     'a   {::pco/op-name      'a
                                           ::pco/dynamic-name 'dyn
                                           ::pco/output       [{:a [:b]}]
                                           ::pco/resolve      (fn [_ _])}
                                     'c   {::pco/op-name      'c
                                           ::pco/dynamic-name 'dyn
                                           ::pco/input        [:b]
                                           ::pco/output       [:c]}}
              ::pci/index-oir       {:a {{} #{'a}}
                                     :c {{:b {}} #{'c}}}
              ::eql/query           [{:a [:c]}]})
           {::pcp/nodes                 {2 {::pco/op-name        'dyn
                                            ::pcp/source-op-name 'a
                                            ::pcp/node-id        2
                                            ::pcp/expects        {:a {:c {}}}
                                            ::pcp/input          {}
                                            ::pcp/foreign-ast    (eql/query->ast [{:a [:c]}])}}
            ::pcp/index-resolver->nodes '{dyn #{2}}
            ::pcp/root                  2
            ::pcp/index-attrs           {:a #{2}}
            ::pcp/index-ast             {:a {:type         :join,
                                             :dispatch-key :a,
                                             :key          :a,
                                             :query        [:c],
                                             :children     [{:type         :prop,
                                                             :dispatch-key :c,
                                                             :key          :c}]}}}))


    (testing "keep intermediate dependency in case external resolvers need it"
      (is (= (compute-run-graph
               {::pci/index-resolvers {'dyn {::pco/op-name           'dyn
                                             ::pco/cache?            false
                                             ::pco/dynamic-resolver? true
                                             ::pco/resolve           (fn [_ _])}
                                       'a   {::pco/op-name      'a
                                             ::pco/dynamic-name 'dyn
                                             ::pco/output       [{:a [:b]}]
                                             ::pco/resolve      (fn [_ _])}
                                       'c   {::pco/op-name      'c
                                             ::pco/dynamic-name 'dyn
                                             ::pco/input        [:b]
                                             ::pco/output       [:c]}
                                       'd   {::pco/op-name 'd
                                             ::pco/input   [:b]
                                             ::pco/output  [:d]}}
                ::pci/index-oir       {:a {{} #{'a}}
                                       :c {{:b {}} #{'c}}
                                       :d {{:b {}} #{'d}}}
                ::eql/query           [{:a [:c :d]}]})
             '{::pcp/index-ast             {:a {:children     [{:dispatch-key :c
                                                                :key          :c
                                                                :type         :prop}
                                                               {:dispatch-key :d
                                                                :key          :d
                                                                :type         :prop}]
                                                :dispatch-key :a
                                                :key          :a
                                                :query        [:c
                                                               :d]
                                                :type         :join}}
               ::pcp/index-attrs           {:a #{4}}
               ::pcp/index-resolver->nodes {dyn #{4}}
               ::pcp/nodes                 {4 {::pco/op-name        dyn
                                               ::pcp/source-op-name a
                                               ::pcp/expects        {:a {:b {}
                                                                         :c {}}}
                                               ::pcp/foreign-ast    {:children [{:children     [{:dispatch-key :c
                                                                                                 :key          :c
                                                                                                 :type         :prop}
                                                                                                {:dispatch-key :b
                                                                                                 :key          :b
                                                                                                 :type         :prop}]
                                                                                 :dispatch-key :a
                                                                                 :key          :a
                                                                                 :query        [:c
                                                                                                :b]
                                                                                 :type         :join}]
                                                                     :type     :root}
                                               ::pcp/input          {}
                                               ::pcp/node-id        4}}
               ::pcp/root                  4})))

    (testing "forward nested inputs"
      (is (= (compute-run-graph
               {::dynamics  {'dyn [{::pco/op-name
                                    'line-items

                                    ::pco/output
                                    [{:order/line-items
                                      [:line-item/id
                                       :line-item/quantity
                                       :line-item/title
                                       :line-item/price-total]}]}]}
                ::resolvers [{::pco/op-name 'total
                              ::pco/input   [{:order/line-items
                                              [:line-item/price-total]}]
                              ::pco/output  [:order/items-total]}]
                ::eql/query [:order/items-total]})
             '{::pcp/nodes                 {1 {::pco/op-name      total,
                                               ::pcp/expects      {:order/items-total {}},
                                               ::pcp/input        {:order/line-items {:line-item/price-total {}}},
                                               ::pcp/node-id      1,
                                               ::pcp/node-parents #{2}},
                                            2 {::pco/op-name        dyn,
                                               ::pcp/expects        {:order/line-items {:line-item/price-total {}}},
                                               ::pcp/input          {},
                                               ::pcp/node-id        2,
                                               ::pcp/source-op-name line-items,
                                               ::pcp/foreign-ast    {:type     :root,
                                                                     :children [{:type         :join,
                                                                                 :key          :order/line-items,
                                                                                 :dispatch-key :order/line-items
                                                                                 :query        [:line-item/price-total]
                                                                                 :children     [{:type         :prop
                                                                                                 :key          :line-item/price-total
                                                                                                 :dispatch-key :line-item/price-total}]}]},
                                               ::pcp/run-next       1}},
               ::pcp/index-ast             {:order/items-total {:type         :prop,
                                                                :dispatch-key :order/items-total,
                                                                :key          :order/items-total},
                                            :order/line-items  {:type         :join,
                                                                :children     [{:type         :prop,
                                                                                :key          :line-item/price-total,
                                                                                :dispatch-key :line-item/price-total}],
                                                                :key          :order/line-items,
                                                                :dispatch-key :order/line-items}},
               ::pcp/index-resolver->nodes {total #{1}, dyn #{2}},
               ::pcp/index-attrs           {:order/items-total #{1},
                                            :order/line-items  #{2}},
               ::pcp/root                  2})))))

(deftest compute-run-graph-dynamic-unions-test
  (testing "forward union"
    (is (= (compute-run-graph
             {::pci/index-resolvers {'dyn {::pco/op-name           'dyn
                                           ::pco/cache?            false
                                           ::pco/dynamic-resolver? true
                                           ::pco/resolve           (fn [_ _])}
                                     'a   {::pco/op-name      'a
                                           ::pco/dynamic-name 'dyn
                                           ::pco/output       [{:a [:b :c]}]
                                           ::pco/provides     {:a {:b {}
                                                                   :c {}}}
                                           ::pco/resolve      (fn [_ _])}}
              ::pci/index-oir       {:a {{} #{'a}}}
              ::eql/query           [{:a {:b [:b]
                                          :c [:c]}}]})
           '{::pcp/nodes                 {1 {::pco/op-name        dyn,
                                             ::pcp/source-op-name a
                                             ::pcp/expects        {:a {:b {:b {}},
                                                                       :c {:c {}}}},
                                             ::pcp/input          {},
                                             ::pcp/node-id        1,
                                             ::pcp/foreign-ast    {:type     :root,
                                                                   :children [{:type         :join,
                                                                               :dispatch-key :a,
                                                                               :key          :a,
                                                                               :query        {:b [:b],
                                                                                              :c [:c]},
                                                                               :children     [{:type     :union,
                                                                                               :children [{:type      :union-entry,
                                                                                                           :union-key :b,
                                                                                                           :children  [{:type         :prop,
                                                                                                                        :key          :b,
                                                                                                                        :dispatch-key :b}]}
                                                                                                          {:type      :union-entry,
                                                                                                           :union-key :c,
                                                                                                           :children  [{:type         :prop,
                                                                                                                        :key          :c,
                                                                                                                        :dispatch-key :c}]}]}]}]}}},
             ::pcp/index-ast             {:a {:type         :join,
                                              :dispatch-key :a,
                                              :key          :a,
                                              :query        {:b [:b], :c [:c]},
                                              :children     [{:type     :union,
                                                              :query    {:b [:b],
                                                                         :c [:c]},
                                                              :children [{:type      :union-entry,
                                                                          :union-key :b,
                                                                          :query     [:b],
                                                                          :children  [{:type         :prop,
                                                                                       :dispatch-key :b,
                                                                                       :key          :b}]}
                                                                         {:type      :union-entry,
                                                                          :union-key :c,
                                                                          :query     [:c],
                                                                          :children  [{:type         :prop,
                                                                                       :dispatch-key :c,
                                                                                       :key          :c}]}]}]}},
             ::pcp/index-resolver->nodes {dyn #{1}},
             ::pcp/index-attrs           {:a #{1}},
             ::pcp/root                  1}))))

(deftest find-run-next-descendants-test
  (testing "return the node if that's the latest"
    (is (= (pcp/find-run-next-descendants
             {::pcp/nodes {1 {::pcp/node-id 1}}}
             {::pcp/node-id 1})
           [{::pcp/node-id 1}]))
    (is (= (pcp/find-run-next-descendants
             {::pcp/nodes {1 {::pcp/node-id  1
                              ::pcp/run-next 2}
                           2 {::pcp/node-id 2}}}
             {::pcp/node-id  1
              ::pcp/run-next 2})
           [{::pcp/node-id  1
             ::pcp/run-next 2}
            {::pcp/node-id 2}]))))

(deftest find-leaf-node-test
  (testing "return the node if that's the latest"
    (is (= (pcp/find-leaf-node
             {::pcp/nodes {1 {::pcp/node-id 1}}}
             {::pcp/node-id 1})
           {::pcp/node-id 1}))

    (is (= (pcp/find-leaf-node
             {::pcp/nodes {1 {::pcp/node-id  1
                              ::pcp/run-next 2}
                           2 {::pcp/node-id 2}}}
             {::pcp/node-id  1
              ::pcp/run-next 2})
           {::pcp/node-id 2}))))

(deftest node-ancestors-test
  (is (= (pcp/node-ancestors
           '{::pcp/nodes {1 {::pcp/node-id 1}
                          2 {::pcp/node-parents #{1}}}}
           2)
         [2 1]))

  (is (= (pcp/node-ancestors
           '{::pcp/nodes {1 {::pcp/node-id 1}
                          2 {::pcp/node-parents #{1}}
                          3 {::pcp/node-parents #{2}}}}
           3)
         [3 2 1]))

  (is (= (pcp/node-ancestors
           '{::pcp/nodes {1 {::pcp/node-id 1}
                          2 {::pcp/node-id 2}
                          3 {::pcp/node-id 3}
                          4 {::pcp/node-parents #{2 1}}
                          5 {::pcp/node-parents #{3}}
                          6 {::pcp/node-parents #{5 4}}}}
           6)
         [6 4 5 1 2 3])))

(deftest node-successors-test
  (testing "leaf"
    (is (= (pcp/node-successors
             '{::pcp/nodes {1 {}}}
             1)
           [1])))

  (testing "chains"
    (is (= (pcp/node-successors
             '{::pcp/nodes {1 {::pcp/run-next 2}
                            2 {}}}
             1)
           [1 2]))

    (is (= (pcp/node-successors
             '{::pcp/nodes {1 {::pcp/run-next 2}
                            2 {::pcp/run-next 3}
                            3 {}}}
             1)
           [1 2 3])))

  (testing "branches"
    (is (= (pcp/node-successors
             '{::pcp/nodes {1 {::pcp/run-and #{2 3}}
                            2 {}
                            3 {}}}
             1)
           [1 3 2]))

    (is (= (pcp/node-successors
             '{::pcp/nodes {1 {::pcp/run-or #{2 3}}
                            2 {}
                            3 {}}}
             1)
           [1 3 2]))

    (is (= (pcp/node-successors
             '{::pcp/nodes {1 {::pcp/run-and #{2 3 4 5}}
                            2 {}
                            3 {}
                            4 {}
                            5 {}}}
             1)
           [1 4 3 2 5])))

  (testing "branch and chains"
    (is (= (pcp/node-successors
             '{::pcp/nodes {1 {::pcp/run-and  #{2 3}
                               ::pcp/run-next 4}
                            2 {}
                            3 {}
                            4 {}}}
             1)
           [1 3 2 4]))))

(deftest remove-node-test
  (testing "remove node and references"
    (is (= (pcp/remove-node
             '{::pcp/nodes                 {1 {::pcp/node-id 1
                                               ::pco/op-name a}}
               ::pcp/index-resolver->nodes {a #{1}}}
             1)
           '{::pcp/nodes                 {}
             ::pcp/index-resolver->nodes {}})))

  (testing "remove after node reference from run-next"
    (is (= (pcp/remove-node
             '{::pcp/nodes                 {1 {::pcp/node-id  1
                                               ::pco/op-name  a
                                               ::pcp/run-next 2}
                                            2 {::pcp/node-id      2
                                               ::pco/op-name      b
                                               ::pcp/node-parents #{1}}}
               ::pcp/index-resolver->nodes {a #{1}
                                            b #{2}}}
             1)
           '{::pcp/nodes                 {2 {::pcp/node-id 2
                                             ::pco/op-name b}}
             ::pcp/index-resolver->nodes {b #{2}}})))

  (testing "remove after node of branch nodes"
    (is (= (pcp/remove-node
             '{::pcp/nodes                 {1 {::pcp/node-id      1
                                               ::pco/op-name      a
                                               ::pcp/node-parents #{3}}
                                            2 {::pcp/node-id      2
                                               ::pco/op-name      b
                                               ::pcp/node-parents #{3}}
                                            3 {::pcp/run-and #{1 2}}}
               ::pcp/index-resolver->nodes {a #{1}
                                            b #{2}}}
             3)
           '{::pcp/nodes                 {1 {::pcp/node-id 1
                                             ::pco/op-name a}
                                          2 {::pcp/node-id 2
                                             ::pco/op-name b}}
             ::pcp/index-resolver->nodes {a #{1} b #{2}}})))

  (testing "trigger error when after node references are still pointing to it"
    (is (thrown?
          #?(:clj AssertionError :cljs js/Error)
          (pcp/remove-node
            '{::pcp/nodes                 {1 {::pcp/node-id      1
                                              ::pco/op-name      a
                                              ::pcp/node-parents #{2}}
                                           2 {::pcp/node-id  2
                                              ::pco/op-name  b
                                              ::pcp/run-next 1}}
              ::pcp/index-resolver->nodes {a #{1}
                                           b #{2}}}
            1)))))

(deftest set-node-run-next-test
  (is (= (pcp/set-node-run-next
           {::pcp/nodes {1 {}
                         2 {}}}
           1
           2)
         {::pcp/nodes {1 {::pcp/run-next 2}
                       2 {::pcp/node-parents #{1}}}}))

  (is (= (pcp/set-node-run-next
           {::pcp/nodes {1 {::pcp/run-next 2}
                         2 {::pcp/node-parents #{1}}}}
           1
           nil)
         {::pcp/nodes {1 {}
                       2 {}}})))

(deftest graph-provides-test
  (is (= (pcp/graph-provides
           {::pcp/index-attrs {:b #{2}, :a #{1}}})
         #{:b :a})))

(deftest merge-unreachable-test
  (is (= (pcp/merge-unreachable
           {}
           {})
         {}))

  (is (= (pcp/merge-unreachable
           {}
           {::pcp/unreachable-paths {:a {}}})
         {::pcp/unreachable-paths {:a {}}})))

(deftest shape-reachable?-test
  (is (false? (pcp/shape-reachable?
                (compute-env
                  {::eql/query             [:scores-sum]
                   ::p.error/lenient-mode? true
                   ::resolvers             '[{::pco/op-name scores-sum
                                              ::pco/input   [{:users [:user/score]}]
                                              ::pco/output  [:scores-sum]}
                                             {::pco/op-name users
                                              ::pco/output  [{:users [:user/id]}]}]})
                {}
                {:users {:user/score {}}})))

  (is (true? (pcp/shape-reachable?
               (compute-env
                 {::eql/query [:scores-sum]
                  ::resolvers '[{::pco/op-name scores-sum
                                 ::pco/input   [{:users [:user/score]}]
                                 ::pco/output  [:scores-sum]}
                                {::pco/op-name users
                                 ::pco/output  [{:users [:user/id]}]}]})
               {}
               {:users {:user/id {}}}))))

(deftest remove-root-node-cluster-test
  (is (= (pcp/remove-root-node-cluster
           {::pcp/nodes {1 {::pcp/node-id  1
                            ::pcp/run-next 2}
                         2 {::pcp/run-and #{3 4}}
                         3 {::pcp/node-id 3}
                         4 {::pcp/node-id 4}
                         5 {::pcp/node-id 5}}}
           [1])
         {::pcp/nodes {5 {::pcp/node-id 5}}})))

(def snaps* (atom []))

(comment
  @snaps*)

(deftest merge-sibling-resolver-nodes-test
  (is (= (pcp/merge-sibling-resolver-nodes
           '#::pcp{:nodes                 {1 {::pco/op-name      dynamic-resolver
                                              ::pcp/expects      {:a {}}
                                              ::pcp/input        {}
                                              ::pcp/node-id      1
                                              ::pcp/foreign-ast  {:type     :root
                                                                  :children [{:type         :prop
                                                                              :dispatch-key :a
                                                                              :key          :a}]}
                                              ::pcp/node-parents #{3}}
                                           2 {::pco/op-name      dynamic-resolver
                                              ::pcp/expects      {:b {}}
                                              ::pcp/input        {}
                                              ::pcp/node-id      2
                                              ::pcp/foreign-ast  {:type     :root
                                                                  :children [{:type         :prop
                                                                              :dispatch-key :b
                                                                              :key          :b}]}
                                              ::pcp/node-parents #{3}}
                                           3 #::pcp{:node-id 3
                                                    :run-and #{1
                                                               2}}}
                   :index-ast             {:a {:type         :prop
                                               :dispatch-key :a
                                               :key          :a}
                                           :b {:type         :prop
                                               :dispatch-key :b
                                               :key          :b}}
                   :index-resolver->nodes {dynamic-resolver #{1 2}}
                   :index-attrs           {:a #{1} :b #{2}}
                   :root                  3}
           {::pcp/snapshots* snaps*}
           3
           #{1 2})
         '#::pcp{:nodes                 {1 {::pco/op-name      dynamic-resolver
                                            ::pcp/expects      {:a {}
                                                                :b {}}
                                            ::pcp/input        {}
                                            ::pcp/node-id      1
                                            ::pcp/foreign-ast  {:type     :root
                                                                :children [{:type         :prop
                                                                            :dispatch-key :a
                                                                            :key          :a}
                                                                           {:type         :prop
                                                                            :dispatch-key :b
                                                                            :key          :b}]}
                                            ::pcp/node-parents #{3}}
                                         3 #::pcp{:node-id 3
                                                  :run-and #{1}}}
                 :index-ast             {:a {:type         :prop
                                             :dispatch-key :a
                                             :key          :a}
                                         :b {:type         :prop
                                             :dispatch-key :b
                                             :key          :b}}
                 :index-resolver->nodes {dynamic-resolver #{1}}
                 :index-attrs           {:a #{1} :b #{1}}
                 :root                  3}))

  (testing "adjust run-next"
    (testing "different run-next"
      (is (= (pcp/merge-sibling-resolver-nodes
               '#::pcp{:nodes                 {1 {::pco/op-name      ab
                                                  ::pcp/expects      {:a {}}
                                                  ::pcp/node-id      1
                                                  ::pcp/node-parents #{3}
                                                  ::pcp/run-next     4}
                                               2 {::pco/op-name      ab
                                                  ::pcp/expects      {:b {}}
                                                  ::pcp/node-id      2
                                                  ::pcp/node-parents #{3}
                                                  ::pcp/run-next     5}
                                               3 #::pcp{:node-id 3
                                                        :run-and #{1 2}}
                                               4 {::pcp/node-parents #{1}}
                                               5 {::pcp/node-parents #{2}}}
                       :index-ast             {:a {:type         :prop
                                                   :dispatch-key :a
                                                   :key          :a}
                                               :b {:type         :prop
                                                   :dispatch-key :b
                                                   :key          :b}}
                       :index-resolver->nodes {ab #{1 2}}
                       :index-attrs           {:a #{1} :b #{2}}
                       :root                  3}
               {::pcp/id-counter (atom 5)}
               3
               #{1 2})
             '#::pcp{:index-ast             {:a {:dispatch-key :a
                                                 :key          :a
                                                 :type         :prop}
                                             :b {:dispatch-key :b
                                                 :key          :b
                                                 :type         :prop}}
                     :index-attrs           {:a #{1}
                                             :b #{1}}
                     :index-resolver->nodes {ab #{1}}
                     :nodes                 {1 {::pco/op-name      ab
                                                ::pcp/expects      {:a {}
                                                                    :b {}}
                                                ::pcp/node-id      1
                                                ::pcp/node-parents #{3}
                                                ::pcp/run-next     6}
                                             3 #::pcp{:node-id 3
                                                      :run-and #{1}}
                                             4 #::pcp{:node-parents #{6}}
                                             5 #::pcp{:node-parents #{6}}
                                             6 #::pcp{:node-id      6
                                                      :node-parents #{1}
                                                      :run-and      #{4
                                                                      5}}}
                     :root                  3})))

    (testing "single next"
      (is (= (pcp/merge-sibling-resolver-nodes
               '#::pcp{:nodes                 {1 {::pco/op-name      ab
                                                  ::pcp/expects      {:a {}}
                                                  ::pcp/node-id      1
                                                  ::pcp/node-parents #{3}}
                                               2 {::pco/op-name      ab
                                                  ::pcp/expects      {:b {}}
                                                  ::pcp/node-id      2
                                                  ::pcp/node-parents #{3}
                                                  ::pcp/run-next     5}
                                               3 #::pcp{:node-id 3
                                                        :run-and #{1 2}}
                                               5 {::pcp/node-parents #{2}}}
                       :index-ast             {:a {:type         :prop
                                                   :dispatch-key :a
                                                   :key          :a}
                                               :b {:type         :prop
                                                   :dispatch-key :b
                                                   :key          :b}}
                       :index-resolver->nodes {ab #{1 2}}
                       :index-attrs           {:a #{1} :b #{2}}
                       :root                  3}
               {::pcp/id-counter (atom 5)}
               3
               #{1 2})
             '#::pcp{:index-ast             {:a {:dispatch-key :a
                                                 :key          :a
                                                 :type         :prop}
                                             :b {:dispatch-key :b
                                                 :key          :b
                                                 :type         :prop}}
                     :index-attrs           {:a #{1}
                                             :b #{1}}
                     :index-resolver->nodes {ab #{1}}
                     :nodes                 {1 {::pco/op-name      ab
                                                ::pcp/expects      {:a {}
                                                                    :b {}}
                                                ::pcp/node-id      1
                                                ::pcp/node-parents #{3}
                                                ::pcp/run-next     5}
                                             3 #::pcp{:node-id 3
                                                      :run-and #{1}}
                                             5 #::pcp{:node-parents #{1}}}
                     :root                  3})))

    (testing "single next already on pivot"
      (is (= (pcp/merge-sibling-resolver-nodes
               '#::pcp{:nodes                 {1 {::pco/op-name      ab
                                                  ::pcp/expects      {:a {}}
                                                  ::pcp/node-id      1
                                                  ::pcp/node-parents #{3}
                                                  ::pcp/run-next     5}
                                               2 {::pco/op-name      ab
                                                  ::pcp/expects      {:b {}}
                                                  ::pcp/node-id      2
                                                  ::pcp/node-parents #{3}}
                                               3 #::pcp{:node-id 3
                                                        :run-and #{1 2}}
                                               5 {::pcp/node-id      5
                                                  ::pcp/node-parents #{1}}}
                       :index-ast             {:a {:type         :prop
                                                   :dispatch-key :a
                                                   :key          :a}
                                               :b {:type         :prop
                                                   :dispatch-key :b
                                                   :key          :b}}
                       :index-resolver->nodes {ab #{1 2}}
                       :index-attrs           {:a #{1} :b #{2}}
                       :root                  3}
               {::pcp/id-counter (atom 5)}
               3
               #{1 2})
             '#::pcp{:index-ast             {:a {:dispatch-key :a
                                                 :key          :a
                                                 :type         :prop}
                                             :b {:dispatch-key :b
                                                 :key          :b
                                                 :type         :prop}}
                     :index-attrs           {:a #{1}
                                             :b #{1}}
                     :index-resolver->nodes {ab #{1}}
                     :nodes                 {1 {::pco/op-name      ab
                                                ::pcp/expects      {:a {}
                                                                    :b {}}
                                                ::pcp/node-id      1
                                                ::pcp/node-parents #{3}
                                                ::pcp/run-next     5}
                                             3 #::pcp{:node-id 3
                                                      :run-and #{1}}
                                             5 #::pcp{:node-id      5
                                                      :node-parents #{1}}}
                     :root                  3}))))

  (testing "3 nodes"
    (is (= (pcp/merge-sibling-resolver-nodes
             '#::pcp{:nodes                 {1 {::pco/op-name      dynamic-resolver
                                                ::pcp/expects      {:a {}}
                                                ::pcp/input        {}
                                                ::pcp/node-id      1
                                                ::pcp/foreign-ast  {:type     :root
                                                                    :children [{:type         :prop
                                                                                :dispatch-key :a
                                                                                :key          :a}]}
                                                ::pcp/node-parents #{3}}
                                             2 {::pco/op-name      dynamic-resolver
                                                ::pcp/expects      {:b {}}
                                                ::pcp/input        {}
                                                ::pcp/node-id      2
                                                ::pcp/foreign-ast  {:type     :root
                                                                    :children [{:type         :prop
                                                                                :dispatch-key :b
                                                                                :key          :b}]}
                                                ::pcp/node-parents #{3}}
                                             3 {::pco/op-name      dynamic-resolver
                                                ::pcp/expects      {:c {}}
                                                ::pcp/input        {}
                                                ::pcp/node-id      3
                                                ::pcp/foreign-ast  {:type     :root
                                                                    :children [{:type         :prop
                                                                                :dispatch-key :c
                                                                                :key          :c}]}
                                                ::pcp/node-parents #{3}}
                                             4 #::pcp{:node-id 4
                                                      :run-and #{1 2 3}}}
                     :index-ast             {:a {:type         :prop
                                                 :dispatch-key :a
                                                 :key          :a}
                                             :b {:type         :prop
                                                 :dispatch-key :b
                                                 :key          :b}
                                             :c {:type         :prop
                                                 :dispatch-key :c
                                                 :key          :c}}
                     :index-resolver->nodes {dynamic-resolver #{1 2 3}}
                     :index-attrs           {:a #{1} :b #{2} :c #{3}}
                     :root                  4}
             {::pcp/snapshots* snaps*}
             4
             #{1 2 3})
           '#::pcp{:nodes                 {1 {::pco/op-name      dynamic-resolver,
                                              ::pcp/expects      {:a {},
                                                                  :c {},
                                                                  :b {}},
                                              ::pcp/input        {},
                                              ::pcp/node-id      1,
                                              ::pcp/foreign-ast  {:type     :root,
                                                                  :children [{:type         :prop,
                                                                              :dispatch-key :a,
                                                                              :key          :a}
                                                                             {:type         :prop,
                                                                              :dispatch-key :c,
                                                                              :key          :c}
                                                                             {:type         :prop,
                                                                              :dispatch-key :b,
                                                                              :key          :b}]},
                                              ::pcp/node-parents #{3}},
                                           4 #::pcp{:node-id 4,
                                                    :run-and #{1
                                                               3
                                                               2}}},
                   :index-ast             {:a {:type         :prop,
                                               :dispatch-key :a,
                                               :key          :a},
                                           :b {:type         :prop,
                                               :dispatch-key :b,
                                               :key          :b},
                                           :c {:type         :prop,
                                               :dispatch-key :c,
                                               :key          :c}},
                   :index-resolver->nodes {dynamic-resolver #{1}},
                   :index-attrs           {:a #{1}, :b #{1}, :c #{1}},
                   :root                  4}))))

(deftest can-merge-sibling-resolver-nodes?-test
  (is (pcp/can-merge-sibling-resolver-nodes?
        {::pcp/nodes {1 {::pcp/node-id          1
                         ::pco/op-name          'ab
                         ::pcp/input            {}
                         ::pcp/expects          {:a {}}
                         ::pcp/source-for-attrs #{:a}}
                      2 {::pcp/node-id          2
                         ::pco/op-name          'ab
                         ::pcp/input            {}
                         ::pcp/expects          {:b {}}
                         ::pcp/source-for-attrs #{:b}}}}
        1 2))

  (is (not (pcp/can-merge-sibling-resolver-nodes?
             {::pcp/nodes {1 {::pcp/node-id          1
                              ::pco/op-name          'ab
                              ::pcp/input            {}
                              ::pcp/expects          {:a {}}
                              ::pcp/source-for-attrs #{:a}}
                           2 {::pcp/node-id          2
                              ::pco/op-name          'ac
                              ::pcp/input            {}
                              ::pcp/expects          {:b {}}
                              ::pcp/source-for-attrs #{:b}}}}
             1 2)))

  (is (not (pcp/can-merge-sibling-resolver-nodes?
             {::pcp/nodes {1 {::pcp/node-id          1
                              ::pco/op-name          'ab
                              ::pcp/input            {}
                              ::pcp/expects          {:a {}}
                              ::pcp/source-for-attrs #{:a}}
                           2 {::pcp/node-id 2
                              ::pcp/run-and #{3 4}}}}
             1 2))))

(deftest optimize-resolver-chain-test
  (is (= (pcp/optimize-resolver-chain
           '#::pcp{:nodes                 {1 {::pco/op-name      dynamic-resolver,
                                              ::pcp/expects      {:c {}},
                                              ::pcp/input        {:b {}},
                                              ::pcp/node-id      1,
                                              ::pcp/node-parents #{2}
                                              ::pcp/foreign-ast  {:type     :root,
                                                                  :children [{:type         :prop,
                                                                              :dispatch-key :c,
                                                                              :key          :c}]}},
                                           2 {::pcp/node-id     2,
                                              ::pco/op-name     dynamic-resolver,
                                              ::pcp/expects     {:b {}},
                                              ::pcp/input       {:a {}},
                                              ::pcp/run-next    1
                                              ::pcp/foreign-ast {:type     :root,
                                                                 :children [{:type         :prop,
                                                                             :dispatch-key :b,
                                                                             :key          :b}]}}}
                   :index-ast             {:c {:type         :prop,
                                               :dispatch-key :c,
                                               :key          :c}}
                   :index-resolver->nodes {dynamic-resolver #{1 2}}
                   :index-attrs           {:b #{2}, :c #{1}}
                   :root                  2}
           {}
           2)
         '#::pcp{:nodes                 {2 {::pcp/node-id     2,
                                            ::pco/op-name     dynamic-resolver,
                                            ::pcp/expects     {:c {}},
                                            ::pcp/input       {:a {}},
                                            ::pcp/foreign-ast {:type     :root,
                                                               :children [{:type         :prop,
                                                                           :dispatch-key :c,
                                                                           :key          :c}]}}}
                 :index-ast             {:c {:type         :prop,
                                             :dispatch-key :c,
                                             :key          :c}}
                 :index-resolver->nodes {dynamic-resolver #{2}}
                 :index-attrs           {:c #{2}}
                 :root                  2}))

  (is (= (pcp/optimize-resolver-chain
           '#::pcp{:nodes                 {1 {::pco/op-name      dynamic-resolver,
                                              ::pcp/expects      {:c {}},
                                              ::pcp/input        {:b {}},
                                              ::pcp/node-id      1,
                                              ::pcp/run-next     3
                                              ::pcp/node-parents #{2}
                                              ::pcp/foreign-ast  {:type     :root,
                                                                  :children [{:type         :prop,
                                                                              :dispatch-key :c,
                                                                              :key          :c}]}},
                                           2 {::pcp/node-id     2,
                                              ::pco/op-name     dynamic-resolver,
                                              ::pcp/expects     {:b {}},
                                              ::pcp/input       {:a {}},
                                              ::pcp/run-next    1
                                              ::pcp/foreign-ast {:type     :root,
                                                                 :children [{:type         :prop,
                                                                             :dispatch-key :b,
                                                                             :key          :b}]}}
                                           3 {::pcp/node-id      3
                                              ::pco/op-name      other-resolver
                                              ::pcp/expects      {:d {}},
                                              ::pcp/input        {:c {}}
                                              ::pcp/node-parents #{1}}}
                   :index-ast             {:c {:type         :prop,
                                               :dispatch-key :c,
                                               :key          :c}}
                   :index-resolver->nodes {dynamic-resolver #{1 2}
                                           other-resolver   #{3}}
                   :index-attrs           {:b #{2}, :c #{1} :d #{3}}
                   :root                  2}
           {}
           2)
         '#::pcp{:nodes                 {2 {::pcp/node-id     2,
                                            ::pco/op-name     dynamic-resolver,
                                            ::pcp/expects     {:c {}},
                                            ::pcp/input       {:a {}},
                                            ::pcp/run-next    3
                                            ::pcp/foreign-ast {:type     :root,
                                                               :children [{:type         :prop,
                                                                           :dispatch-key :c,
                                                                           :key          :c}]}}
                                         3 {::pcp/node-id      3
                                            ::pco/op-name      other-resolver
                                            ::pcp/expects      {:d {}},
                                            ::pcp/input        {:c {}}
                                            ::pcp/node-parents #{2}}}
                 :index-ast             {:c {:type         :prop,
                                             :dispatch-key :c,
                                             :key          :c}}
                 :index-resolver->nodes {dynamic-resolver #{2}
                                         other-resolver   #{3}}
                 :index-attrs           {:c #{2} :d #{3}}
                 :root                  2})))

(deftest optimize-AND-branches-test
  (is (= (pcp/optimize-AND-branches
           '#::pcp{:nodes                 {1 {::pco/op-name      dynamic-resolver,
                                              ::pcp/expects      {:a {}},
                                              ::pcp/input        {},
                                              ::pcp/node-id      1,
                                              ::pcp/foreign-ast  {:type     :root,
                                                                  :children [{:type         :prop,
                                                                              :dispatch-key :a,
                                                                              :key          :a}]},
                                              ::pcp/node-parents #{3}},
                                           2 {::pco/op-name      dynamic-resolver,
                                              ::pcp/expects      {:b {}},
                                              ::pcp/input        {},
                                              ::pcp/node-id      2,
                                              ::pcp/foreign-ast  {:type     :root,
                                                                  :children [{:type         :prop,
                                                                              :dispatch-key :b,
                                                                              :key          :b}]},
                                              ::pcp/node-parents #{3}},
                                           3 #::pcp{:node-id 3,
                                                    :run-and #{1
                                                               2}}},
                   :index-ast             {:a {:type         :prop,
                                               :dispatch-key :a,
                                               :key          :a},
                                           :b {:type         :prop,
                                               :dispatch-key :b,
                                               :key          :b}},
                   :index-resolver->nodes {dynamic-resolver #{1 2}},
                   :index-attrs           {:a #{1}, :b #{2}},
                   :root                  3}
           {::pcp/snapshots* snaps*}
           3)
         '#::pcp{:nodes                 {1 {::pco/op-name     dynamic-resolver
                                            ::pcp/expects     {:a {}
                                                               :b {}}
                                            ::pcp/input       {}
                                            ::pcp/node-id     1
                                            ::pcp/foreign-ast {:type     :root
                                                               :children [{:type         :prop
                                                                           :dispatch-key :a
                                                                           :key          :a}
                                                                          {:type         :prop
                                                                           :dispatch-key :b
                                                                           :key          :b}]}}}
                 :index-ast             {:a {:type         :prop
                                             :dispatch-key :a
                                             :key          :a}
                                         :b {:type         :prop
                                             :dispatch-key :b
                                             :key          :b}}
                 :index-resolver->nodes {dynamic-resolver #{1}}
                 :index-attrs           {:a #{1}
                                         :b #{1}}
                 :root                  1})))

(deftest optimize-OR-subpaths-test
  (is (= (pcp/optimize-OR-sub-paths
           '{::pcp/nodes                 {1 {::pco/op-name      a,
                                             ::pcp/expects      {:a {}},
                                             ::pcp/input        {},
                                             ::pcp/node-id      1,
                                             ::pcp/node-parents #{4}},
                                          2 {::pco/op-name      b,
                                             ::pcp/expects      {:a {}},
                                             ::pcp/input        {:b {}},
                                             ::pcp/node-id      2,
                                             ::pcp/node-parents #{3}},
                                          3 {::pco/op-name      a,
                                             ::pcp/expects      {:b {}},
                                             ::pcp/input        {},
                                             ::pcp/node-id      3,
                                             ::pcp/run-next     2,
                                             ::pcp/node-parents #{4}},
                                          4 {::pcp/expects {:a {}},
                                             ::pcp/node-id 4,
                                             ::pcp/run-or  #{1
                                                             3}}},
             ::pcp/index-ast             {:a {:type         :prop,
                                              :dispatch-key :a,
                                              :key          :a}},
             ::pcp/source-ast            {:type     :root,
                                          :children [{:type         :prop,
                                                      :dispatch-key :a,
                                                      :key          :a}]},
             ::pcp/available-data        nil,
             ::pcp/index-resolver->nodes {a #{1 3}, b #{2}},
             ::pcp/index-attrs           {:a #{1 2}, :b #{3}},
             ::pcp/root                  4}
           {}
           4)
         '{::pcp/nodes                 {2 {::pco/op-name      b,
                                           ::pcp/expects      {:a {}},
                                           ::pcp/input        {:b {}},
                                           ::pcp/node-id      2,
                                           ::pcp/node-parents #{3}},
                                        3 {::pco/op-name      a,
                                           ::pcp/expects      {:a {}
                                                               :b {}},
                                           ::pcp/input        {},
                                           ::pcp/node-id      3,
                                           ::pcp/run-next     2,
                                           ::pcp/node-parents #{4}},
                                        4 {::pcp/expects {:a {}},
                                           ::pcp/node-id 4,
                                           ::pcp/run-or  #{3}}},
           ::pcp/index-ast             {:a {:type         :prop,
                                            :dispatch-key :a,
                                            :key          :a}},
           ::pcp/source-ast            {:type     :root,
                                        :children [{:type         :prop,
                                                    :dispatch-key :a,
                                                    :key          :a}]},
           ::pcp/available-data        nil,
           ::pcp/index-resolver->nodes {a #{3}, b #{2}},
           ::pcp/index-attrs           {:a #{3 2}, :b #{3}},
           ::pcp/root                  4})))

(deftest optimize-nested-OR-test
  (check
    (compute-run-graph
      {::resolvers [{::pco/op-name 'a1
                     ::pco/output  [:a]}
                    {::pco/op-name 'a2
                     ::pco/output  [:a]}
                    {::pco/op-name 'a3
                     ::pco/output  [:a]}
                    {::pco/op-name 'a4
                     ::pco/input   [:b]
                     ::pco/output  [:a]}
                    {::pco/op-name 'b
                     ::pco/output  [:b]}]
       ::eql/query [:a]})
    => '{:com.wsscode.pathom3.connect.planner/nodes                 {3 {:com.wsscode.pathom3.connect.operation/op-name    a3,
                                                                        :com.wsscode.pathom3.connect.planner/expects      {:a {}},
                                                                        :com.wsscode.pathom3.connect.planner/input        {},
                                                                        :com.wsscode.pathom3.connect.planner/node-id      3,
                                                                        :com.wsscode.pathom3.connect.planner/node-parents #{7}},
                                                                     2 {:com.wsscode.pathom3.connect.operation/op-name    a1,
                                                                        :com.wsscode.pathom3.connect.planner/expects      {:a {}},
                                                                        :com.wsscode.pathom3.connect.planner/input        {},
                                                                        :com.wsscode.pathom3.connect.planner/node-id      2,
                                                                        :com.wsscode.pathom3.connect.planner/node-parents #{7}},
                                                                     1 {:com.wsscode.pathom3.connect.operation/op-name    a2,
                                                                        :com.wsscode.pathom3.connect.planner/expects      {:a {}},
                                                                        :com.wsscode.pathom3.connect.planner/input        {},
                                                                        :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                        :com.wsscode.pathom3.connect.planner/node-parents #{7}},
                                                                     5 {:com.wsscode.pathom3.connect.operation/op-name    a4,
                                                                        :com.wsscode.pathom3.connect.planner/expects      {:a {}},
                                                                        :com.wsscode.pathom3.connect.planner/input        {:b {}},
                                                                        :com.wsscode.pathom3.connect.planner/node-id      5,
                                                                        :com.wsscode.pathom3.connect.planner/node-parents #{6}},
                                                                     6 {:com.wsscode.pathom3.connect.operation/op-name    b,
                                                                        :com.wsscode.pathom3.connect.planner/expects      {:b {}},
                                                                        :com.wsscode.pathom3.connect.planner/input        {},
                                                                        :com.wsscode.pathom3.connect.planner/node-id      6,
                                                                        :com.wsscode.pathom3.connect.planner/run-next     5,
                                                                        :com.wsscode.pathom3.connect.planner/node-parents #{7}},
                                                                     7 {:com.wsscode.pathom3.connect.planner/expects {:a {}},
                                                                        :com.wsscode.pathom3.connect.planner/node-id 7,
                                                                        :com.wsscode.pathom3.connect.planner/run-or  #{1
                                                                                                                       6
                                                                                                                       3
                                                                                                                       2}}},
         :com.wsscode.pathom3.connect.planner/index-ast             {:a {:type         :prop,
                                                                         :dispatch-key :a,
                                                                         :key          :a}},
         :com.wsscode.pathom3.connect.planner/index-resolver->nodes {a3 #{3},
                                                                     a1 #{2},
                                                                     a2 #{1},
                                                                     a4 #{5},
                                                                     b  #{6}},
         :com.wsscode.pathom3.connect.planner/index-attrs           {:a #{1 3 2 5}, :b #{6}},
         :com.wsscode.pathom3.connect.planner/root                  7}))

(deftest simplify-branch-node-test
  (is (= (pcp/simplify-branch-node
           '#::pcp{:nodes                 {1 {::pco/op-name      dynamic-resolver,
                                              ::pcp/expects      {:a {},
                                                                  :b {}},
                                              ::pcp/input        {},
                                              ::pcp/node-id      1,
                                              ::pcp/foreign-ast  {:type     :root,
                                                                  :children [{:type         :prop,
                                                                              :dispatch-key :a,
                                                                              :key          :a}
                                                                             {:type         :prop,
                                                                              :dispatch-key :b,
                                                                              :key          :b}]},
                                              ::pcp/node-parents #{3}},
                                           3 #::pcp{:node-id 3,
                                                    :run-and #{1}}},
                   :index-ast             {:a {:type         :prop,
                                               :dispatch-key :a,
                                               :key          :a},
                                           :b {:type         :prop,
                                               :dispatch-key :b,
                                               :key          :b}},
                   :index-resolver->nodes {dynamic-resolver #{1}},
                   :index-attrs           {:a #{1}, :b #{1}},
                   :root                  3}
           {}
           3)
         '#::pcp{:nodes                 {1 {::pco/op-name     dynamic-resolver,
                                            ::pcp/expects     {:a {}
                                                               :b {}}
                                            ::pcp/input       {},
                                            ::pcp/node-id     1,
                                            ::pcp/foreign-ast {:type     :root,
                                                               :children [{:type         :prop,
                                                                           :dispatch-key :a,
                                                                           :key          :a}
                                                                          {:type         :prop,
                                                                           :dispatch-key :b,
                                                                           :key          :b}]},}},
                 :index-ast             {:a {:type         :prop,
                                             :dispatch-key :a,
                                             :key          :a},
                                         :b {:type         :prop,
                                             :dispatch-key :b,
                                             :key          :b}},
                 :index-resolver->nodes {dynamic-resolver #{1}},
                 :index-attrs           {:a #{1},
                                         :b #{1}},
                 :root                  1}))

  (is (= (pcp/simplify-branch-node
           '{::pcp/nodes                 {1 {::pco/op-name      dynamic-resolver,
                                             ::pcp/expects      {:a {},
                                                                 :b {}},
                                             ::pcp/input        {},
                                             ::pcp/node-id      1,
                                             ::pcp/foreign-ast  {:type     :root,
                                                                 :children [{:type         :prop,
                                                                             :dispatch-key :a,
                                                                             :key          :a}
                                                                            {:type         :prop,
                                                                             :dispatch-key :b,
                                                                             :key          :b}]},
                                             ::pcp/node-parents #{3}},
                                          3 {::pcp/node-id 3,
                                             ::pcp/run-or  #{1}}},
             ::pcp/index-ast             {:a {:type         :prop,
                                              :dispatch-key :a,
                                              :key          :a},
                                          :b {:type         :prop,
                                              :dispatch-key :b,
                                              :key          :b}},
             ::pcp/index-resolver->nodes {dynamic-resolver #{1}},
             ::pcp/index-attrs           {:a #{1}, :b #{1}},
             ::pcp/root                  3}
           {}
           3)
         '#::pcp{:nodes                 {1 {::pco/op-name     dynamic-resolver,
                                            ::pcp/expects     {:a {}
                                                               :b {}}
                                            ::pcp/input       {},
                                            ::pcp/node-id     1,
                                            ::pcp/foreign-ast {:type     :root,
                                                               :children [{:type         :prop,
                                                                           :dispatch-key :a,
                                                                           :key          :a}
                                                                          {:type         :prop,
                                                                           :dispatch-key :b,
                                                                           :key          :b}]},}},
                 :index-ast             {:a {:type         :prop,
                                             :dispatch-key :a,
                                             :key          :a},
                                         :b {:type         :prop,
                                             :dispatch-key :b,
                                             :key          :b}},
                 :index-resolver->nodes {dynamic-resolver #{1}},
                 :index-attrs           {:a #{1},
                                         :b #{1}},
                 :root                  1}))

  (testing "move connection to that item"
    (is (= (pcp/simplify-branch-node
             '{::pcp/nodes                 {1 {::pco/op-name      dynamic-resolver,
                                               ::pcp/expects      {:a {},
                                                                   :b {}},
                                               ::pcp/input        {},
                                               ::pcp/node-id      1,
                                               ::pcp/foreign-ast  {:type     :root,
                                                                   :children [{:type         :prop,
                                                                               :dispatch-key :a,
                                                                               :key          :a}
                                                                              {:type         :prop,
                                                                               :dispatch-key :b,
                                                                               :key          :b}]},
                                               ::pcp/node-parents #{3}},
                                            3 {::pcp/node-id      3,
                                               ::pcp/run-and      #{1}
                                               ::pcp/node-parents #{4}}
                                            4 {::pcp/node-id 4,
                                               ::pcp/run-and #{3}}},
               ::pcp/index-ast             {:a {:type         :prop,
                                                :dispatch-key :a,
                                                :key          :a},
                                            :b {:type         :prop,
                                                :dispatch-key :b,
                                                :key          :b}},
               ::pcp/index-resolver->nodes {dynamic-resolver #{1}},
               ::pcp/index-attrs           {:a #{1}, :b #{1}},
               ::pcp/root                  4}
             {}
             3)
           '{::pcp/nodes                 {1 {::pco/op-name      dynamic-resolver,
                                             ::pcp/expects      {:a {},
                                                                 :b {}},
                                             ::pcp/input        {},
                                             ::pcp/node-id      1,
                                             ::pcp/foreign-ast  {:type     :root,
                                                                 :children [{:type         :prop,
                                                                             :dispatch-key :a,
                                                                             :key          :a}
                                                                            {:type         :prop,
                                                                             :dispatch-key :b,
                                                                             :key          :b}]},
                                             ::pcp/node-parents #{4}},
                                          4 {::pcp/node-id 4,
                                             ::pcp/run-and #{1}}},
             ::pcp/index-ast             {:a {:type         :prop,
                                              :dispatch-key :a,
                                              :key          :a},
                                          :b {:type         :prop,
                                              :dispatch-key :b,
                                              :key          :b}},
             ::pcp/index-resolver->nodes {dynamic-resolver #{1}},
             ::pcp/index-attrs           {:a #{1}, :b #{1}},
             ::pcp/root                  4}))))

(deftest transfer-node-parents-test
  (is (= (pcp/transfer-node-parents
           '#::pcp{:nodes                 {1 {::pco/op-name      dynamic-resolver,
                                              ::pcp/expects      {:a {},
                                                                  :b {}},
                                              ::pcp/input        {},
                                              ::pcp/node-id      1,
                                              ::pcp/foreign-ast  {:type     :root,
                                                                  :children [{:type         :prop,
                                                                              :dispatch-key :a,
                                                                              :key          :a}
                                                                             {:type         :prop,
                                                                              :dispatch-key :b,
                                                                              :key          :b}]},
                                              ::pcp/node-parents #{3}},
                                           3 #::pcp{:node-id 3,
                                                    :run-and #{1}}},
                   :index-ast             {:a {:type         :prop,
                                               :dispatch-key :a,
                                               :key          :a},
                                           :b {:type         :prop,
                                               :dispatch-key :b,
                                               :key          :b}},
                   :index-resolver->nodes {dynamic-resolver #{1}},
                   :index-attrs           {:a #{1}, :b #{1}},
                   :root                  3}
           1 3)
         '#::pcp{:nodes                 {1 {::pco/op-name      dynamic-resolver,
                                            ::pcp/expects      {:a {},
                                                                :b {}},
                                            ::pcp/input        {},
                                            ::pcp/node-id      1,
                                            ::pcp/foreign-ast  {:type     :root,
                                                                :children [{:type         :prop,
                                                                            :dispatch-key :a,
                                                                            :key          :a}
                                                                           {:type         :prop,
                                                                            :dispatch-key :b,
                                                                            :key          :b}]},
                                            ::pcp/node-parents #{3}},
                                         3 #::pcp{:node-id 3,
                                                  :run-and #{1}}},
                 :index-ast             {:a {:type         :prop,
                                             :dispatch-key :a,
                                             :key          :a},
                                         :b {:type         :prop,
                                             :dispatch-key :b,
                                             :key          :b}},
                 :index-resolver->nodes {dynamic-resolver #{1}},
                 :index-attrs           {:a #{1}, :b #{1}},
                 :root                  1}))

  (is (= (pcp/transfer-node-parents
           '#::pcp{:nodes {1 {::pcp/node-parents #{2}},
                           2 {::pcp/run-next 1}
                           3 {::pcp/node-parents #{4 5}}
                           4 {::pcp/run-next 3}
                           5 {::pcp/run-and #{3}}},
                   :root  3}
           1 3)
         '#::pcp{:nodes {1 {::pcp/node-parents #{2 4 5}},
                         2 {::pcp/run-next 1}
                         3 {}
                         4 {::pcp/run-next 1}
                         5 {::pcp/run-and #{1}}},
                 :root  1})))

(deftest find-root-resolver-nodes-test
  (is (= (pcp/find-root-resolver-nodes
           {::pcp/nodes {1 {::pco/op-name 'res}}
            ::pcp/root  1})
         #{1}))

  (is (= (pcp/find-root-resolver-nodes
           {::pcp/nodes {1 {::pco/op-name 'res}
                         2 {::pcp/run-and #{1}}}
            ::pcp/root  2})
         #{1}))

  (is (= (pcp/find-root-resolver-nodes
           {::pcp/nodes {1 {::pco/op-name 'res}
                         2 {::pco/op-name 'other}
                         3 {::pcp/run-and #{1 2}}}
            ::pcp/root  3})
         #{1 2}))

  (is (= (pcp/find-root-resolver-nodes
           {::pcp/nodes {1 {::pco/op-name 'res}
                         2 {::pcp/run-and #{4 5}}
                         3 {::pcp/run-and #{1 2}}
                         4 {::pco/op-name 'other}
                         5 {::pco/op-name 'other2}}
            ::pcp/root  3})
         #{1 4 5})))

(deftest remove-node-expects-index-attrs-test
  (is (= (pcp/remove-node-expects-index-attrs
           {::pcp/nodes       {1 {::pcp/expects {:a {} :b {}}}}
            ::pcp/index-attrs {:a #{1} :b #{1}}}
           1)
         {::pcp/nodes       {1 {::pcp/expects {:a {}
                                               :b {}}}}
          ::pcp/index-attrs {}})))

(deftest remove-node-edges-test
  (is (= (pcp/remove-node-edges
           {::pcp/nodes {1 {::pcp/node-id 1
                            ::pcp/run-and #{2}}
                         2 {::pcp/node-id      2
                            ::pcp/node-parents #{1}}}}
           2)
         {::pcp/nodes {1 {::pcp/node-id 1
                          ::pcp/run-and #{}}
                       2 {::pcp/node-id 2}}}))

  (is (= (pcp/remove-node-edges
           {::pcp/nodes {1 {::pcp/node-id 1
                            ::pcp/run-and #{2}}
                         2 {::pcp/node-id      2
                            ::pcp/node-parents #{1 3}}
                         3 {::pcp/node-id  3
                            ::pcp/run-next 2}}}
           2)
         {::pcp/nodes {1 {::pcp/node-id 1
                          ::pcp/run-and #{}}
                       2 {::pcp/node-id 2}
                       3 {::pcp/node-id 3}}}))

  (is (= (pcp/remove-node-edges
           {::pcp/nodes {1 {::pcp/node-id 1
                            ::pcp/run-and #{2}}
                         2 {::pcp/node-id      2
                            ::pcp/node-parents #{1 3}
                            ::pcp/run-next     4}
                         3 {::pcp/node-id  3
                            ::pcp/run-next 2}
                         4 {::pcp/node-id      4
                            ::pcp/node-parents #{2}}}}
           2)
         {::pcp/nodes {1 {::pcp/node-id 1
                          ::pcp/run-and #{}}
                       2 {::pcp/node-id 2}
                       3 {::pcp/node-id 3}
                       4 {::pcp/node-id 4}}})))
