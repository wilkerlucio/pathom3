(ns com.wsscode.pathom3.connect.planner-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.misc.coll :as coll]
    ;[com.wsscode.pathom3.attribute :as p.attr]
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
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {2 {:com.wsscode.pathom3.connect.operation/op-name    a2,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:a {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {},
                                                                             :com.wsscode.pathom3.connect.planner/node-id      2,
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{3}},
                                                                          1 {:com.wsscode.pathom3.connect.operation/op-name    a,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:a {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {},
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{3}},
                                                                          3 #:com.wsscode.pathom3.connect.planner{:expects {:a {}},
                                                                                                                  :node-id 3,
                                                                                                                  :run-or  #{1
                                                                                                                             2}}},
                                                  :index-ast             {:a {:type         :prop,
                                                                              :dispatch-key :a,
                                                                              :key          :a}},
                                                  :index-resolver->nodes {a2 #{2}, a #{1}},
                                                  :index-attrs           {:a #{1 2}},
                                                  :root                  3})))

  (testing "partial failure with OR"
    (is (= (compute-run-graph
             {::pci/index-oir      '{:bar/id     {{:foo/id {}}   #{bar-id}
                                                  {:bar/year {}} #{get-bar-id-from-year}}
                                     :foo/number {{:foo/id {}} #{bar-id}}
                                     :bar/year   {{:bar/id {}} #{get-year}}}
              ::eql/query          [:foo/number :bar/id :bar/year]
              ::pcp/available-data {:foo/id {}}})
           '#:com.wsscode.pathom3.connect.planner{:index-ast             {:bar/id     {:dispatch-key :bar/id
                                                                                       :key          :bar/id
                                                                                       :type         :prop}
                                                                          :bar/year   {:dispatch-key :bar/year
                                                                                       :key          :bar/year
                                                                                       :type         :prop}
                                                                          :foo/number {:dispatch-key :foo/number
                                                                                       :key          :foo/number
                                                                                       :type         :prop}}
                                                  :index-attrs           {:bar/id     #{1}
                                                                          :bar/year   #{5}
                                                                          :foo/number #{1}}
                                                  :index-resolver->nodes {bar-id   #{1}
                                                                          get-year #{5}}
                                                  :nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name bar-id
                                                                             :com.wsscode.pathom3.connect.planner/expects   {:bar/id     {}
                                                                                                                             :foo/number {}}
                                                                             :com.wsscode.pathom3.connect.planner/input     #:foo{:id {}}
                                                                             :com.wsscode.pathom3.connect.planner/node-id   1
                                                                             :com.wsscode.pathom3.connect.planner/run-next  5}
                                                                          5 {:com.wsscode.pathom3.connect.operation/op-name    get-year
                                                                             :com.wsscode.pathom3.connect.planner/expects      #:bar{:year {}}
                                                                             :com.wsscode.pathom3.connect.planner/input        #:bar{:id {}}
                                                                             :com.wsscode.pathom3.connect.planner/node-id      5
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{1}}}
                                                  :root                  1})))

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
             ::pcp/root                  3}))))

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
               '{:com.wsscode.pathom3.connect.planner/nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name a,
                                                                                :com.wsscode.pathom3.connect.planner/expects   {:a {}},
                                                                                :com.wsscode.pathom3.connect.planner/input     {},
                                                                                :com.wsscode.pathom3.connect.planner/node-id   1}},
                 :com.wsscode.pathom3.connect.planner/index-ast             {:a {:type         :prop,
                                                                                 :dispatch-key :a,
                                                                                 :key          :a},
                                                                             :b {:type         :prop,
                                                                                 :dispatch-key :b,
                                                                                 :key          :b,
                                                                                 :params       {:com.wsscode.pathom3.connect.operation/optional? true}}},
                 :com.wsscode.pathom3.connect.planner/index-resolver->nodes {a #{1}},
                 :com.wsscode.pathom3.connect.planner/index-attrs           {:a #{1}},
                 :com.wsscode.pathom3.connect.planner/root                  1,
                 :com.wsscode.pathom3.connect.planner/unreachable-paths     {:b {}}})))

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
           {::pcp/nodes             {}
            ::pcp/unreachable-paths {:a {}}
            ::pcp/index-ast         {:a {:dispatch-key :a
                                         :key          :a
                                         :type         :prop}}}))

    (testing "broken chain"
      (is (= (compute-run-graph
               {::pci/index-oir         '{:b {{:a {}} #{b}}}
                ::eql/query             [:b]
                ::p.error/lenient-mode? true})
             '#::pcp{:nodes             {}
                     :unreachable-paths {:b {}, :a {}}
                     :index-ast         {:b {:dispatch-key :b
                                             :key          :b
                                             :type         :prop}}}))

      (is (= (compute-run-graph
               {::pci/index-oir         '{:b {{:a {}} #{b1 b}}}
                ::eql/query             [:b]
                ::p.error/lenient-mode? true})
             '#::pcp{:nodes             {}
                     :unreachable-paths {:a {} :b {}}
                     :index-ast         {:b {:dispatch-key :b
                                             :key          :b
                                             :type         :prop}}}))

      (is (= (compute-run-graph
               {::resolvers             [{::pco/op-name 'a
                                          ::pco/output  [:a]}
                                         {::pco/op-name 'b
                                          ::pco/input   [:a]
                                          ::pco/output  [:b]}]
                ::eql/query             [:b]
                ::out                   {::pcp/unreachable-paths {:a {}}}
                ::p.error/lenient-mode? true})
             '#::pcp{:nodes             {}
                     :unreachable-paths {:a {} :b {}}
                     :index-ast         {:b {:dispatch-key :b
                                             :key          :b
                                             :type         :prop}}}))

      (is (= (compute-run-graph
               {::resolvers             [{::pco/op-name 'b
                                          ::pco/input   [:a]
                                          ::pco/output  [:b]}
                                         {::pco/op-name 'c
                                          ::pco/input   [:b]
                                          ::pco/output  [:c]}]
                ::eql/query             [:c]
                ::p.error/lenient-mode? true})
             '#::pcp{:nodes             {}
                     :unreachable-paths {:a {} :b {} :c {}}
                     :index-ast         {:c {:dispatch-key :c
                                             :key          :c
                                             :type         :prop}}}))

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
             '#::pcp{:nodes             {}
                     :unreachable-paths {:c {}, :b {}, :a {}}
                     :index-ast         {:c {:dispatch-key :c
                                             :key          :c
                                             :type         :prop}}})))))

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
             '{:com.wsscode.pathom3.connect.planner/nodes {},
               :com.wsscode.pathom3.connect.planner/index-ast {doit {:dispatch-key doit,
                                                                     :key doit,
                                                                     :params {},
                                                                     :type :call,
                                                                     :query [:done],
                                                                     :children [{:type :prop,
                                                                                 :dispatch-key :done,
                                                                                 :key :done}],
                                                                     :com.wsscode.pathom3.connect.planner/foreign-ast {:type :root,
                                                                                                                       :children [{:type :prop,
                                                                                                                                   :key :done,
                                                                                                                                   :dispatch-key :done}]}}},
               :com.wsscode.pathom3.connect.planner/mutations [doit]})))

    (testing "attribute extended from mutation, but still in the same foreign"
      (is (= (compute-run-graph
               {::dynamics  {'dyn [(pco/mutation 'doit {::pco/output [:done]} (fn [_ _]))
                                   (pbir/alias-resolver :done :done?)]}
                ::eql/query [{(list 'doit {}) [:done?]}]})
             '{:com.wsscode.pathom3.connect.planner/nodes {},
               :com.wsscode.pathom3.connect.planner/index-ast {doit {:dispatch-key doit,
                                                                     :key doit,
                                                                     :params {},
                                                                     :type :call,
                                                                     :query [:done?],
                                                                     :children [{:type :prop,
                                                                                 :dispatch-key :done?,
                                                                                 :key :done?}],
                                                                     :com.wsscode.pathom3.connect.planner/foreign-ast {:type :root,
                                                                                                                       :children [{:type :prop,
                                                                                                                                   :key :done?,
                                                                                                                                   :dispatch-key :done?}]}}},
               :com.wsscode.pathom3.connect.planner/mutations [doit]})))

    (testing "attribute extended from mutation locally"
      (is (= (compute-run-graph
               {::dynamics  {'dyn [(pco/mutation 'doit {::pco/output [:done]} (fn [_ _]))]}
                ::resolvers [(pbir/alias-resolver :done :done?)]
                ::eql/query [{(list 'doit {}) [:done?]}]})
             '{:com.wsscode.pathom3.connect.planner/index-ast {doit {:children                                        [{:dispatch-key :done?
                                                                                                                        :key          :done?
                                                                                                                        :type         :prop}]
                                                                     :com.wsscode.pathom3.connect.planner/foreign-ast {:children [{:dispatch-key :done
                                                                                                                                   :key          :done
                                                                                                                                   :type         :prop}]
                                                                                                                       :type     :root}
                                                                     :dispatch-key                                    doit
                                                                     :key                                             doit
                                                                     :params                                          {}
                                                                     :query                                           [:done?]
                                                                     :type                                            :call}}
               :com.wsscode.pathom3.connect.planner/mutations [doit]
               :com.wsscode.pathom3.connect.planner/nodes     {}})))))

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
           '#::pcp{:nodes             {},
                   :unreachable-paths {:b {}, :a {}},
                   :index-ast         {:a {:type         :prop,
                                           :dispatch-key :a,
                                           :key          :a}}}))

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
           '#::pcp{:nodes             {}
                   :unreachable-paths {:c {}, :b {}, :a {}}
                   :index-ast         {:a {:type         :prop,
                                           :dispatch-key :a,
                                           :key          :a}}}))

    (testing "partial cycle"
      (is (= (compute-run-graph
               {::pci/index-oir '{:a {{:c {}} #{a}
                                      {}      #{a1}}
                                  :b {{:a {}} #{b}}
                                  :c {{:b {}} #{c}}
                                  :d {{} #{d}}}
                ::eql/query     [:c :a]})
             '#:com.wsscode.pathom3.connect.planner{:index-ast             {:a {:dispatch-key :a
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
                                                    :nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    c
                                                                               :com.wsscode.pathom3.connect.planner/expects      {:c {}}
                                                                               :com.wsscode.pathom3.connect.planner/input        {:b {}}
                                                                               :com.wsscode.pathom3.connect.planner/node-id      1
                                                                               :com.wsscode.pathom3.connect.planner/node-parents #{2}}
                                                                            2 {:com.wsscode.pathom3.connect.operation/op-name    b
                                                                               :com.wsscode.pathom3.connect.planner/expects      {:b {}}
                                                                               :com.wsscode.pathom3.connect.planner/input        {:a {}}
                                                                               :com.wsscode.pathom3.connect.planner/node-id      2
                                                                               :com.wsscode.pathom3.connect.planner/node-parents #{4}
                                                                               :com.wsscode.pathom3.connect.planner/run-next     1}
                                                                            4 {:com.wsscode.pathom3.connect.operation/op-name a1
                                                                               :com.wsscode.pathom3.connect.planner/expects   {:a {}}
                                                                               :com.wsscode.pathom3.connect.planner/input     {}
                                                                               :com.wsscode.pathom3.connect.planner/node-id   4
                                                                               :com.wsscode.pathom3.connect.planner/run-next  2}}
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
           '{::pcp/nodes             {}
             ::pcp/unreachable-paths {:scores-sum {}
                                      :users      {:user/score {}}}
             ::pcp/index-ast         {:scores-sum {:type         :prop,
                                                   :dispatch-key :scores-sum,
                                                   :key          :scores-sum}}})))

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
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    scores-sum,
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:scores-sum {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {:users #:user{:score {}}},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{2},},
                                                                          2 {:com.wsscode.pathom3.connect.operation/op-name users,
                                                                             :com.wsscode.pathom3.connect.planner/node-id   2,
                                                                             :com.wsscode.pathom3.connect.planner/expects   {:users {}},
                                                                             :com.wsscode.pathom3.connect.planner/input     {},
                                                                             :com.wsscode.pathom3.connect.planner/run-next  1}},
                                                  :index-ast             {:scores-sum {:type         :prop,
                                                                                       :dispatch-key :scores-sum,
                                                                                       :key          :scores-sum},
                                                                          :users      {:type         :join,
                                                                                       :children     [{:type         :prop,
                                                                                                       :key          :user/score,
                                                                                                       :dispatch-key :user/score}],
                                                                                       :key          :users,
                                                                                       :dispatch-key :users}},
                                                  :index-resolver->nodes {scores-sum #{1},
                                                                          users      #{2}},
                                                  :index-attrs           {:scores-sum #{1}, :users #{2}},
                                                  :root                  2})))

  (testing "mark bad paths regarding nested inputs"
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
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1                  {:com.wsscode.pathom3.connect.operation/op-name    z,
                                                                                              :com.wsscode.pathom3.connect.planner/expects      {:z {}},
                                                                                              :com.wsscode.pathom3.connect.planner/input        {:a {:b {}}},
                                                                                              :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                                              :com.wsscode.pathom3.connect.planner/node-parents #{5}},
                                                                          4                  {:com.wsscode.pathom3.connect.operation/op-name    a3,
                                                                                              :com.wsscode.pathom3.connect.planner/expects      {:a {}},
                                                                                              :com.wsscode.pathom3.connect.planner/input        {},
                                                                                              :com.wsscode.pathom3.connect.planner/node-id      4,
                                                                                              :com.wsscode.pathom3.connect.planner/node-parents #{5}},
                                                                          #?(:clj 3 :cljs 2) {:com.wsscode.pathom3.connect.operation/op-name     a1,
                                                                                              :com.wsscode.pathom3.connect.planner/expects       {:a {}},
                                                                                              :com.wsscode.pathom3.connect.planner/input         {},
                                                                                              :com.wsscode.pathom3.connect.planner/node-id       #?(:clj 3 :cljs 2),
                                                                                              :com.wsscode.pathom3.connect.planner/node-parents  #{5},
                                                                                              :com.wsscode.pathom3.connect.planner/invalid-node? true},
                                                                          #?(:clj 2 :cljs 3) {:com.wsscode.pathom3.connect.operation/op-name     a2,
                                                                                              :com.wsscode.pathom3.connect.planner/expects       {:a {}},
                                                                                              :com.wsscode.pathom3.connect.planner/input         {},
                                                                                              :com.wsscode.pathom3.connect.planner/node-id       #?(:clj 2 :cljs 3),
                                                                                              :com.wsscode.pathom3.connect.planner/node-parents  #{5},
                                                                                              :com.wsscode.pathom3.connect.planner/invalid-node? true},
                                                                          5                  #:com.wsscode.pathom3.connect.planner{:expects  {:a {}},
                                                                                                                                   :node-id  5,
                                                                                                                                   :run-or   #{4
                                                                                                                                               3
                                                                                                                                               2},
                                                                                                                                   :run-next 1}},
                                                  :index-ast             {:z {:type         :prop,
                                                                              :dispatch-key :z,
                                                                              :key          :z},
                                                                          :a {:type         :join,
                                                                              :children     [{:type         :prop,
                                                                                              :key          :b,
                                                                                              :dispatch-key :b}],
                                                                              :key          :a,
                                                                              :dispatch-key :a}},
                                                  :index-resolver->nodes {z  #{1},
                                                                          a3 #{4},
                                                                          a1 #{#?(:clj 3 :cljs 2)},
                                                                          a2 #{#?(:clj 2 :cljs 3)}},
                                                  :index-attrs           {:z #{1}, :a #{4 3 2}},
                                                  :root                  5})))

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
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name scores-sum,
                                                                             :com.wsscode.pathom3.connect.planner/node-id   1,
                                                                             :com.wsscode.pathom3.connect.planner/expects   {:scores-sum {}},
                                                                             :com.wsscode.pathom3.connect.planner/input     {:users #:user{:score {}}}}},
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
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    scores-sum,
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:scores-sum {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {:users #:user{:score {}},
                                                                                                                                :other {}},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{3},},
                                                                          3 {:com.wsscode.pathom3.connect.operation/op-name other,
                                                                             :com.wsscode.pathom3.connect.planner/node-id   3,
                                                                             :com.wsscode.pathom3.connect.planner/expects   {:other {}},
                                                                             :com.wsscode.pathom3.connect.planner/input     {},
                                                                             :com.wsscode.pathom3.connect.planner/run-next  1}},
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
           '{:com.wsscode.pathom3.connect.planner/nodes
             {1
              {:com.wsscode.pathom3.connect.operation/op-name scores-sum,
               :com.wsscode.pathom3.connect.planner/node-id   1,
               :com.wsscode.pathom3.connect.planner/expects   {:scores-sum {}},
               :com.wsscode.pathom3.connect.planner/input
               {:users {:user/score {}}}}},
             :com.wsscode.pathom3.connect.planner/index-resolver->nodes
             {scores-sum #{1}},
             :com.wsscode.pathom3.connect.planner/index-ast
             {:scores-sum
              {:type :prop, :dispatch-key :scores-sum, :key :scores-sum}},
             :com.wsscode.pathom3.connect.planner/root        1,
             :com.wsscode.pathom3.connect.planner/index-attrs {:scores-sum #{1}}})))

  (testing "multiple resolvers for the same root but different sub queries"
    (is (= (compute-run-graph
             (-> {::eql/query           [:scores-sum :total-max-score]
                  ::resolvers           '[{::pco/op-name scores-sum
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
           '{:com.wsscode.pathom3.connect.planner/nodes
             {1
              {:com.wsscode.pathom3.connect.operation/op-name    scores-sum,
               :com.wsscode.pathom3.connect.planner/node-id      1,
               :com.wsscode.pathom3.connect.planner/expects      {:scores-sum {}},
               :com.wsscode.pathom3.connect.planner/input
               {:users {:user/score {}}},
               :com.wsscode.pathom3.connect.planner/node-parents #{2}},
              2
              {:com.wsscode.pathom3.connect.operation/op-name users,
               :com.wsscode.pathom3.connect.planner/node-id   2,
               :com.wsscode.pathom3.connect.planner/expects   {:users {}},
               :com.wsscode.pathom3.connect.planner/input     {},
               :com.wsscode.pathom3.connect.planner/run-next  1}},
             :com.wsscode.pathom3.connect.planner/index-resolver->nodes
             {scores-sum #{1}, users #{2}},
             ::pcp/unreachable-paths
             {:total-max-score {}, :users {:user/max-score {}}},
             :com.wsscode.pathom3.connect.planner/index-ast
             {:scores-sum
              {:type :prop, :dispatch-key :scores-sum, :key :scores-sum},
              :total-max-score
              {:type         :prop,
               :dispatch-key :total-max-score,
               :key          :total-max-score}
              :users
              {:type         :join
               :key          :users
               :dispatch-key :users
               :children     [{:type         :prop
                               :key          :user/score
                               :dispatch-key :user/score}]}},
             :com.wsscode.pathom3.connect.planner/index-attrs
             {:scores-sum #{1}, :users #{2}},
             :com.wsscode.pathom3.connect.planner/root 2})))

  (testing "self output reference in input"
    (is (= (compute-run-graph
             (-> {::eql/query             [:b]
                  ::resolvers             '[{::pco/op-name x
                                             ::pco/input   [{:a [:b]}]
                                             ::pco/output  [:b]}]
                  ::p.error/lenient-mode? true}))
           '#:com.wsscode.pathom3.connect.planner{:nodes             {},
                                                  :index-ast         {:b {:type         :prop,
                                                                          :dispatch-key :b,
                                                                          :key          :b}},
                                                  :unreachable-paths {:a {}, :b {}}}))

    (is (= (compute-run-graph
             (-> {::eql/query             [:b]
                  ::pcp/available-data    {:a {}}
                  ::resolvers             '[{::pco/op-name x
                                             ::pco/input   [{:a [:b]}]
                                             ::pco/output  [:b]}]
                  ::p.error/lenient-mode? true}))
           '#:com.wsscode.pathom3.connect.planner{:nodes             {},
                                                  :index-ast         {:b {:type         :prop,
                                                                          :dispatch-key :b,
                                                                          :key          :b}},
                                                  :unreachable-paths {:a {:b {}}, :b {}}})))

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
           '#:com.wsscode.pathom3.connect.planner{:index-ast             {:age-sum    {:dispatch-key :age-sum
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
                                                  :index-attrs           {:age-sum    #{4}
                                                                          :scores-sum #{1}
                                                                          :users      #{2}}
                                                  :index-resolver->nodes {age-sum    #{4}
                                                                          scores-sum #{1}
                                                                          users      #{2}}
                                                  :nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    scores-sum
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:scores-sum {}}
                                                                             :com.wsscode.pathom3.connect.planner/input        {:users #:user{:score {}}}
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{9}}
                                                                          2 {:com.wsscode.pathom3.connect.operation/op-name users
                                                                             :com.wsscode.pathom3.connect.planner/expects   {:users {}}
                                                                             :com.wsscode.pathom3.connect.planner/input     {}
                                                                             :com.wsscode.pathom3.connect.planner/node-id   2
                                                                             :com.wsscode.pathom3.connect.planner/run-next  9}
                                                                          4 {:com.wsscode.pathom3.connect.operation/op-name    age-sum
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:age-sum {}}
                                                                             :com.wsscode.pathom3.connect.planner/input        {:users #:user{:age {}}}
                                                                             :com.wsscode.pathom3.connect.planner/node-id      4
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{9}}
                                                                          9 #:com.wsscode.pathom3.connect.planner{:node-id      9
                                                                                                                  :node-parents #{2}
                                                                                                                  :run-and      #{1
                                                                                                                                  4}}}
                                                  :root                  2})))

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
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    nested-input-recursive,
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:names {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {:name     {},
                                                                                                                                :children {}},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{2},},
                                                                          2 {:com.wsscode.pathom3.connect.operation/op-name from-name,
                                                                             :com.wsscode.pathom3.connect.planner/node-id   2,
                                                                             :com.wsscode.pathom3.connect.planner/expects   {:children {}},
                                                                             :com.wsscode.pathom3.connect.planner/input     {:name {}},
                                                                             :com.wsscode.pathom3.connect.planner/run-next  1}},
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
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    total-score,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:total-score {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {:users {}},
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{3}},
                                                                          3 {:com.wsscode.pathom3.connect.operation/op-name users,
                                                                             :com.wsscode.pathom3.connect.planner/expects   {:users {}},
                                                                             :com.wsscode.pathom3.connect.planner/input     {},
                                                                             :com.wsscode.pathom3.connect.planner/node-id   3,
                                                                             :com.wsscode.pathom3.connect.planner/run-next  1}},
                                                  :index-ast             {:total-score {:type         :prop,
                                                                                        :dispatch-key :total-score,
                                                                                        :key          :total-score},
                                                                          :users       {:type         :join,
                                                                                        :children     [{:type         :prop,
                                                                                                        :key          :user/score,
                                                                                                        :dispatch-key :user/score
                                                                                                        :params       {::pco/optional? true}}],
                                                                                        :key          :users,
                                                                                        :dispatch-key :users}},
                                                  :index-resolver->nodes {total-score #{1},
                                                                          users       #{3}},
                                                  :index-attrs           {:total-score #{1}, :users #{3 2}},
                                                  :root                  3}))

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
           '{:com.wsscode.pathom3.connect.planner/nodes {1 {:com.wsscode.pathom3.connect.operation/op-name total-score,
                                                            :com.wsscode.pathom3.connect.planner/expects {:total-score {}},
                                                            :com.wsscode.pathom3.connect.planner/input {:users {:foo {}}},
                                                            :com.wsscode.pathom3.connect.planner/node-id 1,
                                                            :com.wsscode.pathom3.connect.planner/node-parents #{4}},
                                                         4 {:com.wsscode.pathom3.connect.operation/op-name users,
                                                            :com.wsscode.pathom3.connect.planner/expects {:users {}},
                                                            :com.wsscode.pathom3.connect.planner/input {},
                                                            :com.wsscode.pathom3.connect.planner/node-id 4,
                                                            :com.wsscode.pathom3.connect.planner/run-next 1}},
             :com.wsscode.pathom3.connect.planner/index-ast {:total-score {:type :prop,
                                                                           :dispatch-key :total-score,
                                                                           :key :total-score},
                                                             :users {:type :join,
                                                                     :children [{:type :join,
                                                                                 :key :foo,
                                                                                 :dispatch-key :foo,
                                                                                 :children [{:type :prop,
                                                                                             :key :bar,
                                                                                             :dispatch-key :bar,
                                                                                             :params {:com.wsscode.pathom3.connect.operation/optional? true}}],}
                                                                                {:type :prop,
                                                                                 :key :user/score,
                                                                                 :dispatch-key :user/score,
                                                                                 :params {:com.wsscode.pathom3.connect.operation/optional? true}}],
                                                                     :key :users,
                                                                     :dispatch-key :users}},
             :com.wsscode.pathom3.connect.planner/index-resolver->nodes {total-score #{1},
                                                                         users #{4}},
             :com.wsscode.pathom3.connect.planner/index-attrs {:total-score #{1}, :users #{4 2}},
             :com.wsscode.pathom3.connect.planner/root 4})))

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
           '{::pcp/nodes                 {1 {::pco/op-name      z,
                                             ::pcp/expects      {:z {}},
                                             ::pcp/input        {:items {:c {},
                                                                         :b {}}},
                                             ::pcp/node-id      1,
                                             ::pcp/node-parents #{2}},
                                          2 {::pco/op-name  items,
                                             ::pcp/expects  {:items {}},
                                             ::pcp/input    {},
                                             ::pcp/node-id  2,
                                             ::pcp/run-next 1}},
             ::pcp/index-ast             {:z     {:type         :prop,
                                                  :dispatch-key :z,
                                                  :key          :z},
                                          :items {:type         :join,
                                                  :children     [{:type         :prop,
                                                                  :key          :c,
                                                                  :dispatch-key :c}
                                                                 {:type         :prop,
                                                                  :key          :b,
                                                                  :dispatch-key :b}],
                                                  :key          :items,
                                                  :dispatch-key :items}},
             ::pcp/index-resolver->nodes {z #{1}, items #{2}},
             ::pcp/index-attrs           {:z #{1}, :items #{2}},
             ::pcp/root                  2})))

  (testing "nested plan failure"
    (is (thrown-with-msg?
          #?(:clj Throwable :cljs :default)
          #"Pathom can't find a path for the following elements in the query: \[:c] at path \[:a]"
          (compute-run-graph
            {::resolvers [{::pco/op-name 'nested-provider
                           ::pco/output  [{:a [:b]}]}
                          {::pco/op-name 'nested-requires
                           ::pco/input   [{:a [:c]}]
                           ::pco/output  [:d]}]

             :com.wsscode.pathom3.path/path []

             ::eql/query [:d]})))))

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
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    foo,
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:foo {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {:x {}},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{2},},
                                                                          2 {:com.wsscode.pathom3.connect.operation/op-name x,
                                                                             :com.wsscode.pathom3.connect.planner/node-id   2,
                                                                             :com.wsscode.pathom3.connect.planner/expects   {:x {}},
                                                                             :com.wsscode.pathom3.connect.planner/input     {},
                                                                             :com.wsscode.pathom3.connect.planner/run-next  1}},
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
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    foo,
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:foo {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {:x {}},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{4},},
                                                                          2 {:com.wsscode.pathom3.connect.operation/op-name    x,
                                                                             :com.wsscode.pathom3.connect.planner/node-id      2,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:x {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{4}},
                                                                          3 {:com.wsscode.pathom3.connect.operation/op-name    y,
                                                                             :com.wsscode.pathom3.connect.planner/node-id      3,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:y {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{4}},
                                                                          4 #:com.wsscode.pathom3.connect.planner{:node-id  4,
                                                                                                                  :run-and  #{3
                                                                                                                              2},
                                                                                                                  :run-next 1}},
                                                  :index-resolver->nodes {foo #{1}, x #{2}, y #{3}},
                                                  :index-ast             {:foo {:type         :prop,
                                                                                :dispatch-key :foo,
                                                                                :key          :foo}},
                                                  :index-attrs           {:y #{3}, :foo #{1}, :x #{2}},
                                                  :root                  4})))

  (testing "only optional"
    (testing "unavailable"
      (is (= (compute-run-graph
               (-> {::eql/query             [:foo]
                    ::resolvers             [{::pco/op-name 'foo
                                              ::pco/input   [(pco/? :y)]
                                              ::pco/output  [:foo]}]
                    ::p.error/lenient-mode? true}))
             '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name foo,
                                                                               :com.wsscode.pathom3.connect.planner/node-id   1,
                                                                               :com.wsscode.pathom3.connect.planner/expects   {:foo {}},
                                                                               :com.wsscode.pathom3.connect.planner/input     {},}},
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
             '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    foo,
                                                                               :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                               :com.wsscode.pathom3.connect.planner/expects      {:foo {}},
                                                                               :com.wsscode.pathom3.connect.planner/input        {},
                                                                               :com.wsscode.pathom3.connect.planner/node-parents #{2},},
                                                                            2 {:com.wsscode.pathom3.connect.operation/op-name y,
                                                                               :com.wsscode.pathom3.connect.planner/node-id   2,
                                                                               :com.wsscode.pathom3.connect.planner/expects   {:y {}},
                                                                               :com.wsscode.pathom3.connect.planner/input     {},
                                                                               :com.wsscode.pathom3.connect.planner/run-next  1}},
                                                    :index-resolver->nodes {foo #{1}, y #{2}},
                                                    :index-ast             {:foo {:type         :prop,
                                                                                  :dispatch-key :foo,
                                                                                  :key          :foo}},
                                                    :index-attrs           {:y #{2}, :foo #{1}},
                                                    :root                  2})))))

(deftest compute-run-graph-placeholders-test
  (testing "just placeholder"
    (is (= (compute-run-graph
             {::pci/index-oir '{:a {{} #{a}}}
              ::eql/query     [{:>/p1 [:a]}]})
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name a,
                                                                             :com.wsscode.pathom3.connect.planner/node-id   1,
                                                                             :com.wsscode.pathom3.connect.planner/expects   {:a {}},
                                                                             :com.wsscode.pathom3.connect.planner/input     {},}},
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
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    a,
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:a {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{3}},
                                                                          2 {:com.wsscode.pathom3.connect.operation/op-name    b,
                                                                             :com.wsscode.pathom3.connect.planner/node-id      2,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:b {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{3}},
                                                                          3 #:com.wsscode.pathom3.connect.planner{:node-id 3,
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
           '#:com.wsscode.pathom3.connect.planner{:index-ast             #:>{:p1 {:children     [{:dispatch-key :a
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
                                                  :nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name a
                                                                             :com.wsscode.pathom3.connect.planner/expects   {:a {}}
                                                                             :com.wsscode.pathom3.connect.planner/input     {}
                                                                             :com.wsscode.pathom3.connect.planner/node-id   1}}
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
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    a,
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:a {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{3}},
                                                                          2 {:com.wsscode.pathom3.connect.operation/op-name    b,
                                                                             :com.wsscode.pathom3.connect.planner/node-id      2,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:b {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{3}},
                                                                          3 #:com.wsscode.pathom3.connect.planner{:node-id 3,,
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
             '#:com.wsscode.pathom3.connect.planner{:index-ast             {:a {:dispatch-key :a
                                                                                :key          :a
                                                                                :params       {:x 1}
                                                                                :type         :prop}
                                                                            :b {:dispatch-key :b
                                                                                :key          :b
                                                                                :type         :prop}}
                                                    :index-attrs           {:a #{1}
                                                                            :b #{1}}
                                                    :index-resolver->nodes {a #{1}}
                                                    :nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name a
                                                                               :com.wsscode.pathom3.connect.planner/expects   {:a {}
                                                                                                                               :b {}}
                                                                               :com.wsscode.pathom3.connect.planner/input     {}
                                                                               :com.wsscode.pathom3.connect.planner/node-id   1
                                                                               :com.wsscode.pathom3.connect.planner/params    {:x 1}}}
                                                    :root                  1})))))

(deftest compute-run-graph-optimize-test
  (testing "optimize AND nodes"
    (is (= (compute-run-graph
             {::pci/index-oir {:a {{} #{'x}}
                               :b {{} #{'x}}}
              ::eql/query     [:a :b]})
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name x,
                                                                             :com.wsscode.pathom3.connect.planner/expects   {:a {},
                                                                                                                             :b {}},
                                                                             :com.wsscode.pathom3.connect.planner/input     {},
                                                                             :com.wsscode.pathom3.connect.planner/node-id   1}},
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
             '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name x,
                                                                               :com.wsscode.pathom3.connect.planner/expects   {:a {},
                                                                                                                               :b {},
                                                                                                                               :c {}},
                                                                               :com.wsscode.pathom3.connect.planner/input     {},
                                                                               :com.wsscode.pathom3.connect.planner/node-id   1}},
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
             '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    x,
                                                                               :com.wsscode.pathom3.connect.planner/expects      {:a {},
                                                                                                                                  :c {},
                                                                                                                                  :f {}},
                                                                               :com.wsscode.pathom3.connect.planner/input        {},
                                                                               :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                               :com.wsscode.pathom3.connect.planner/node-parents #{7}},
                                                                            2 {:com.wsscode.pathom3.connect.operation/op-name    y,
                                                                               :com.wsscode.pathom3.connect.planner/expects      {:b {}},
                                                                               :com.wsscode.pathom3.connect.planner/input        {},
                                                                               :com.wsscode.pathom3.connect.planner/node-id      2,
                                                                               :com.wsscode.pathom3.connect.planner/node-parents #{7}},
                                                                            4 {:com.wsscode.pathom3.connect.operation/op-name    z,
                                                                               :com.wsscode.pathom3.connect.planner/expects      {:d {},
                                                                                                                                  :e {}},
                                                                               :com.wsscode.pathom3.connect.planner/input        {},
                                                                               :com.wsscode.pathom3.connect.planner/node-id      4,
                                                                               :com.wsscode.pathom3.connect.planner/node-parents #{7}},
                                                                            7 #:com.wsscode.pathom3.connect.planner{:node-id 7,
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
         '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    c,
                                                                           :com.wsscode.pathom3.connect.planner/expects      {:c {}},
                                                                           :com.wsscode.pathom3.connect.planner/input        {:a {},
                                                                                                                              :b {}},
                                                                           :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                           :com.wsscode.pathom3.connect.planner/node-parents #{4}},
                                                                        2 {:com.wsscode.pathom3.connect.operation/op-name    x,
                                                                           :com.wsscode.pathom3.connect.planner/expects      {:a {},
                                                                                                                              :b {}},
                                                                           :com.wsscode.pathom3.connect.planner/input        {},
                                                                           :com.wsscode.pathom3.connect.planner/node-id      2,
                                                                           :com.wsscode.pathom3.connect.planner/node-parents #{4}},
                                                                        4 #:com.wsscode.pathom3.connect.planner{:node-id  4,
                                                                                                                :run-and  #{2},
                                                                                                                :run-next 1}},
                                                :index-ast             {:c {:type         :prop,
                                                                            :dispatch-key :c,
                                                                            :key          :c}},
                                                :index-resolver->nodes {c #{1}, x #{2}},
                                                :index-attrs           {:c #{1}, :a #{2}, :b #{2}},
                                                :root                  4}))

  (is (= (compute-run-graph
           {::pci/index-oir {:a {{:z {}} #{'x}}
                             :b {{:z {}} #{'x}}
                             :z {{} #{'z}}
                             :c {{:a {} :b {}} #{'c}}}
            ::eql/query     [:c]})
         '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    c,
                                                                           :com.wsscode.pathom3.connect.planner/expects      {:c {}},
                                                                           :com.wsscode.pathom3.connect.planner/input        {:a {},
                                                                                                                              :b {}},
                                                                           :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                           :com.wsscode.pathom3.connect.planner/node-parents #{6}},
                                                                        2 {:com.wsscode.pathom3.connect.operation/op-name    x,
                                                                           :com.wsscode.pathom3.connect.planner/expects      {:a {},
                                                                                                                              :b {}},
                                                                           :com.wsscode.pathom3.connect.planner/input        {:z {}},
                                                                           :com.wsscode.pathom3.connect.planner/node-id      2,
                                                                           :com.wsscode.pathom3.connect.planner/node-parents #{3}},
                                                                        3 {:com.wsscode.pathom3.connect.operation/op-name    z,
                                                                           :com.wsscode.pathom3.connect.planner/expects      {:z {}},
                                                                           :com.wsscode.pathom3.connect.planner/input        {},
                                                                           :com.wsscode.pathom3.connect.planner/node-id      3,
                                                                           :com.wsscode.pathom3.connect.planner/run-next     2,
                                                                           :com.wsscode.pathom3.connect.planner/node-parents #{6}},
                                                                        6 #:com.wsscode.pathom3.connect.planner{:node-id  6,
                                                                                                                :run-and  #{3},
                                                                                                                :run-next 1}},
                                                :index-ast             {:c {:type         :prop,
                                                                            :dispatch-key :c,
                                                                            :key          :c}},
                                                :index-resolver->nodes {c #{1}, x #{2}, z #{3}},
                                                :index-attrs           {:c #{1}, :a #{2}, :z #{3}, :b #{2}},
                                                :root                  6}))

  (testing "optimize AND not at root"
    (is (= (compute-run-graph
             {::pci/index-oir {:a {{:z {}} #{'x}}
                               :b {{:z {}} #{'x}}
                               :z {{} #{'z}}}
              ::eql/query     [:a :b]})
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    x,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:a {},
                                                                                                                                :b {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {:z {}},
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{2}},
                                                                          2 {:com.wsscode.pathom3.connect.operation/op-name z,
                                                                             :com.wsscode.pathom3.connect.planner/expects   {:z {}},
                                                                             :com.wsscode.pathom3.connect.planner/input     {},
                                                                             :com.wsscode.pathom3.connect.planner/node-id   2,
                                                                             :com.wsscode.pathom3.connect.planner/run-next  1}},
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
             '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    ax,
                                                                               :com.wsscode.pathom3.connect.planner/expects      {:a {}},
                                                                               :com.wsscode.pathom3.connect.planner/input        {:x {}},
                                                                               :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                               :com.wsscode.pathom3.connect.planner/node-parents #{2}},
                                                                            2 {:com.wsscode.pathom3.connect.operation/op-name    x,
                                                                               :com.wsscode.pathom3.connect.planner/expects      {:x {}},
                                                                               :com.wsscode.pathom3.connect.planner/input        {},
                                                                               :com.wsscode.pathom3.connect.planner/node-id      2,
                                                                               :com.wsscode.pathom3.connect.planner/run-next     1,
                                                                               :com.wsscode.pathom3.connect.planner/node-parents #{7}},
                                                                            3 {:com.wsscode.pathom3.connect.operation/op-name    ayz,
                                                                               :com.wsscode.pathom3.connect.planner/expects      {:a {}},
                                                                               :com.wsscode.pathom3.connect.planner/input        {:y {},
                                                                                                                                  :z {}},
                                                                               :com.wsscode.pathom3.connect.planner/node-id      3,
                                                                               :com.wsscode.pathom3.connect.planner/node-parents #{6}},
                                                                            4 {:com.wsscode.pathom3.connect.operation/op-name    yz,
                                                                               :com.wsscode.pathom3.connect.planner/expects      {:y {},
                                                                                                                                  :z {}},
                                                                               :com.wsscode.pathom3.connect.planner/input        {},
                                                                               :com.wsscode.pathom3.connect.planner/node-id      4,
                                                                               :com.wsscode.pathom3.connect.planner/node-parents #{6}},
                                                                            6 #:com.wsscode.pathom3.connect.planner{:node-id      6,
                                                                                                                    :run-and      #{4},
                                                                                                                    :run-next     3,
                                                                                                                    :node-parents #{7}},
                                                                            7 #:com.wsscode.pathom3.connect.planner{:expects {:a {}},
                                                                                                                    :node-id 7,
                                                                                                                    :run-or  #{6
                                                                                                                               2}}},
                                                    :index-ast             {:a {:type         :prop,
                                                                                :dispatch-key :a,
                                                                                :key          :a}},
                                                    :index-resolver->nodes {ax  #{1},
                                                                            x   #{2},
                                                                            ayz #{3},
                                                                            yz  #{4}},
                                                    :index-attrs           {:a #{1 3},
                                                                            :x #{2},
                                                                            :y #{4},
                                                                            :z #{4}},
                                                    :root                  7})))

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
           {::pcp/nodes             {}
            ::pcp/unreachable-paths {:db/id          {}
                                     :release/script {}}
            ::pcp/index-ast         {:release/script {:type         :prop,
                                                      :dispatch-key :release/script,
                                                      :key          :release/script}}})))

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

             '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name   dynamic-resolver
                                                                               ::pcp/source-op-name                             a
                                                                               :com.wsscode.pathom3.connect.planner/expects     {:a {}}
                                                                               :com.wsscode.pathom3.connect.planner/input       {}
                                                                               :com.wsscode.pathom3.connect.planner/node-id     1
                                                                               :com.wsscode.pathom3.connect.planner/foreign-ast {:type     :root
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
               {::pci/index-resolvers {'dynamic-resolver
                                       {::pco/op-name           'dynamic-resolver
                                        ::pco/cache?            false
                                        ::pco/dynamic-resolver? true
                                        ::pco/resolve           (fn [_ _])}}
                ::pci/index-oir       {:release/script {{:db/id {}} #{'dynamic-resolver}}}
                ::pcp/available-data  {:db/id {}}
                ::eql/query           [(list :release/script {:foo "bar"})]})

             {::pcp/nodes                 {1 {::pco/op-name        'dynamic-resolver
                                              ::pcp/node-id        1
                                              ::pcp/source-op-name 'dynamic-resolver
                                              ::pcp/expects        {:release/script {}}
                                              ::pcp/input          {:db/id {}}
                                              ::pcp/params         {:foo "bar"}
                                              ::pcp/foreign-ast    {:children [{:dispatch-key :release/script
                                                                                :key          :release/script
                                                                                :params       {:foo "bar"}
                                                                                :type         :prop}]
                                                                    :type     :root}}}
              ::pcp/index-resolver->nodes {'dynamic-resolver #{1}}
              ::pcp/root                  1
              ::pcp/index-attrs           {:release/script #{1}}

              ::pcp/index-ast             {:release/script {:type         :prop,
                                                            :dispatch-key :release/script,
                                                            :key          :release/script
                                                            :params       {:foo "bar"}}}}))))

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

           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    dynamic-resolver,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:release/script {},
                                                                                                                                :label/type     {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        #:db{:id {}},
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                             :com.wsscode.pathom3.connect.planner/foreign-ast  {:type     :root,
                                                                                                                                :children [{:type         :prop,
                                                                                                                                            :dispatch-key :release/script,
                                                                                                                                            :key          :release/script}
                                                                                                                                           {:type         :prop,
                                                                                                                                            :dispatch-key :label/type,
                                                                                                                                            :key          :label/type}]},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{2}},
                                                                          2 {:com.wsscode.pathom3.connect.operation/op-name id,
                                                                             :com.wsscode.pathom3.connect.planner/expects   #:db{:id {}},
                                                                             :com.wsscode.pathom3.connect.planner/input     {},
                                                                             :com.wsscode.pathom3.connect.planner/node-id   2,
                                                                             :com.wsscode.pathom3.connect.planner/run-next  1}},
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
                                             ::pcp/node-parents #{5}},
                                          2 {::pco/op-name      dynamic-resolver,
                                             ::pcp/expects      {:b {},
                                                                 :c {}},
                                             ::pcp/input        {:a {}},
                                             ::pcp/node-id      2,
                                             ::pcp/foreign-ast  {:type     :root,
                                                                 :children [{:type         :prop,
                                                                             :key          :b,
                                                                             :dispatch-key :b}
                                                                            {:type         :prop,
                                                                             :key          :c,
                                                                             :dispatch-key :c}]},
                                             ::pcp/node-parents #{5}},
                                          5 {::pcp/node-id  5,
                                             ::pcp/run-and  #{2},
                                             ::pcp/run-next 1}},
             ::pcp/index-ast             {:d {:type         :prop,
                                              :dispatch-key :d,
                                              :key          :d}},
             ::pcp/index-resolver->nodes {other-resolver   #{1},
                                          dynamic-resolver #{2}},
             ::pcp/index-attrs           {:d #{1}, :b #{2}, :c #{2}},
             ::pcp/root                  5})))

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

           '{:com.wsscode.pathom3.connect.planner/nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    dynamic-resolver,
                                                                            :com.wsscode.pathom3.connect.planner/expects      {:release/script {},
                                                                                                                               :label/type     {}},
                                                                            :com.wsscode.pathom3.connect.planner/input        {:db/id {}},
                                                                            :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                            :com.wsscode.pathom3.connect.planner/foreign-ast  {:type     :root,
                                                                                                                               :children [{:type         :prop,
                                                                                                                                           :dispatch-key :release/script,
                                                                                                                                           :key          :release/script}
                                                                                                                                          {:type         :prop,
                                                                                                                                           :dispatch-key :label/type,
                                                                                                                                           :key          :label/type}]},
                                                                            :com.wsscode.pathom3.connect.planner/node-parents #{2}},
                                                                         2 {:com.wsscode.pathom3.connect.operation/op-name id,
                                                                            :com.wsscode.pathom3.connect.planner/expects   {:db/id {}},
                                                                            :com.wsscode.pathom3.connect.planner/input     {},
                                                                            :com.wsscode.pathom3.connect.planner/node-id   2,
                                                                            :com.wsscode.pathom3.connect.planner/run-next  1}},
             :com.wsscode.pathom3.connect.planner/index-ast             {:release/script {:type         :prop,
                                                                                          :dispatch-key :release/script,
                                                                                          :key          :release/script},
                                                                         :label/type     {:type         :prop,
                                                                                          :dispatch-key :label/type,
                                                                                          :key          :label/type}},
             :com.wsscode.pathom3.connect.planner/index-resolver->nodes {dynamic-resolver #{1},
                                                                         id               #{2}},
             :com.wsscode.pathom3.connect.planner/index-attrs           {:release/script #{1},
                                                                         :db/id          #{2},
                                                                         :label/type     #{1}},
             :com.wsscode.pathom3.connect.planner/root                  2})))

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

           '{:com.wsscode.pathom3.connect.planner/nodes                 {2 {:com.wsscode.pathom3.connect.operation/op-name   dynamic-resolver,
                                                                            :com.wsscode.pathom3.connect.planner/expects     {:b {}},
                                                                            :com.wsscode.pathom3.connect.planner/input       {},
                                                                            :com.wsscode.pathom3.connect.planner/node-id     2,
                                                                            :com.wsscode.pathom3.connect.planner/foreign-ast {:type     :root,
                                                                                                                              :children [{:type         :prop,
                                                                                                                                          :dispatch-key :b,
                                                                                                                                          :key          :b}]}}},
             :com.wsscode.pathom3.connect.planner/index-ast             {:b {:type         :prop,
                                                                             :dispatch-key :b,
                                                                             :key          :b}},
             :com.wsscode.pathom3.connect.planner/index-resolver->nodes {dynamic-resolver #{2}},
             :com.wsscode.pathom3.connect.planner/index-attrs           {:b #{2}},
             :com.wsscode.pathom3.connect.planner/root                  2}))

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

           '{:com.wsscode.pathom3.connect.planner/nodes                 {3 {:com.wsscode.pathom3.connect.operation/op-name   dynamic-resolver,
                                                                            :com.wsscode.pathom3.connect.planner/expects     {:c {}},
                                                                            :com.wsscode.pathom3.connect.planner/input       {},
                                                                            :com.wsscode.pathom3.connect.planner/node-id     3,
                                                                            :com.wsscode.pathom3.connect.planner/foreign-ast {:type     :root,
                                                                                                                              :children [{:type         :prop,
                                                                                                                                          :dispatch-key :c,
                                                                                                                                          :key          :c}]}}},
             :com.wsscode.pathom3.connect.planner/index-ast             {:c {:type         :prop,
                                                                             :dispatch-key :c,
                                                                             :key          :c}},
             :com.wsscode.pathom3.connect.planner/index-resolver->nodes {dynamic-resolver #{3}},
             :com.wsscode.pathom3.connect.planner/index-attrs           {:c #{3}},
             :com.wsscode.pathom3.connect.planner/root                  3}))

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

           '{:com.wsscode.pathom3.connect.planner/nodes                 {2 {:com.wsscode.pathom3.connect.operation/op-name    dynamic-resolver,
                                                                            :com.wsscode.pathom3.connect.planner/expects      {:b {}},
                                                                            :com.wsscode.pathom3.connect.planner/input        {:z {}},
                                                                            :com.wsscode.pathom3.connect.planner/node-id      2,
                                                                            :com.wsscode.pathom3.connect.planner/foreign-ast  {:type     :root,
                                                                                                                               :children [{:type         :prop,
                                                                                                                                           :dispatch-key :b,
                                                                                                                                           :key          :b}]},
                                                                            :com.wsscode.pathom3.connect.planner/node-parents #{3}},
                                                                         3 {:com.wsscode.pathom3.connect.operation/op-name z,
                                                                            :com.wsscode.pathom3.connect.planner/expects   {:z {}},
                                                                            :com.wsscode.pathom3.connect.planner/input     {},
                                                                            :com.wsscode.pathom3.connect.planner/node-id   3,
                                                                            :com.wsscode.pathom3.connect.planner/run-next  2}},
             :com.wsscode.pathom3.connect.planner/index-ast             {:b {:type         :prop,
                                                                             :dispatch-key :b,
                                                                             :key          :b}},
             :com.wsscode.pathom3.connect.planner/index-resolver->nodes {dynamic-resolver #{2},
                                                                         z                #{3}},
             :com.wsscode.pathom3.connect.planner/index-attrs           {:b #{2}, :z #{3}},
             :com.wsscode.pathom3.connect.planner/root                  3}))

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

             '{:com.wsscode.pathom3.connect.planner/nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    z,
                                                                              :com.wsscode.pathom3.connect.planner/expects      {:z {}},
                                                                              :com.wsscode.pathom3.connect.planner/input        {:b {}},
                                                                              :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                              :com.wsscode.pathom3.connect.planner/node-parents #{3}},
                                                                           3 {:com.wsscode.pathom3.connect.operation/op-name   dynamic-resolver,
                                                                              :com.wsscode.pathom3.connect.planner/expects     {:b {}},
                                                                              :com.wsscode.pathom3.connect.planner/input       {},
                                                                              :com.wsscode.pathom3.connect.planner/node-id     3,
                                                                              :com.wsscode.pathom3.connect.planner/foreign-ast {:type     :root,
                                                                                                                                :children [{:type         :prop,
                                                                                                                                            :key          :b,
                                                                                                                                            :dispatch-key :b}]},
                                                                              :com.wsscode.pathom3.connect.planner/run-next    1}},
               :com.wsscode.pathom3.connect.planner/index-ast             {:z {:type         :prop,
                                                                               :dispatch-key :z,
                                                                               :key          :z}},
               :com.wsscode.pathom3.connect.planner/index-resolver->nodes {z                #{1},
                                                                           dynamic-resolver #{3}},
               :com.wsscode.pathom3.connect.planner/index-attrs           {:z #{1}, :b #{3}},
               :com.wsscode.pathom3.connect.planner/root                  3})))

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
             '{:com.wsscode.pathom3.connect.planner/nodes                 {5 {:com.wsscode.pathom3.connect.operation/op-name   dyn,
                                                                              :com.wsscode.pathom3.connect.planner/expects     {:e {}},
                                                                              :com.wsscode.pathom3.connect.planner/input       {},
                                                                              :com.wsscode.pathom3.connect.planner/node-id     5,
                                                                              :com.wsscode.pathom3.connect.planner/foreign-ast {:type     :root,
                                                                                                                                :children [{:type         :prop,
                                                                                                                                            :dispatch-key :e,
                                                                                                                                            :key          :e}]}}},
               :com.wsscode.pathom3.connect.planner/index-ast             {:e {:type         :prop,
                                                                               :dispatch-key :e,
                                                                               :key          :e}},
               :com.wsscode.pathom3.connect.planner/index-resolver->nodes {dyn #{5}},
               :com.wsscode.pathom3.connect.planner/index-attrs           {:e #{5}},
               :com.wsscode.pathom3.connect.planner/root                  5}))))

  ; not sure if I'm going though with this
  #_
  (testing "transient attributes"
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
           '{:com.wsscode.pathom3.connect.planner/nodes                 {2 {:com.wsscode.pathom3.connect.operation/op-name   dyn,
                                                                            :com.wsscode.pathom3.connect.planner/expects     {:tb {}},
                                                                            :com.wsscode.pathom3.connect.planner/input       {:tid {}},
                                                                            :com.wsscode.pathom3.connect.planner/node-id     2,
                                                                            :com.wsscode.pathom3.connect.planner/foreign-ast {:type     :root,
                                                                                                                              :children [{:type         :prop,
                                                                                                                                          :dispatch-key :tb,
                                                                                                                                          :key          :tb}]}}},
             :com.wsscode.pathom3.connect.planner/index-ast             {:tb   {:type         :prop,
                                                                                :dispatch-key :tb,
                                                                                :key          :tb},
                                                                         :type {:type         :prop,
                                                                                :dispatch-key :type,
                                                                                :key          :type}},
             :com.wsscode.pathom3.connect.planner/index-resolver->nodes {dyn #{2}},
             :com.wsscode.pathom3.connect.planner/index-attrs           {:tb #{2}},
             :com.wsscode.pathom3.connect.planner/root                  2}))))

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
           {::pcp/nodes                 {2 {::pco/op-name     'dyn
                                            ::pcp/source-op-name 'a
                                            ::pcp/node-id     2
                                            ::pcp/expects     {:a {:c {}}}
                                            ::pcp/input       {}
                                            ::pcp/foreign-ast (eql/query->ast [{:a [:c]}])}}
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
             '{:com.wsscode.pathom3.connect.planner/index-ast             {:a {:children     [{:dispatch-key :c
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
               :com.wsscode.pathom3.connect.planner/index-attrs           {:a #{4}}
               :com.wsscode.pathom3.connect.planner/index-resolver->nodes {dyn #{4}}
               :com.wsscode.pathom3.connect.planner/nodes                 {4 {:com.wsscode.pathom3.connect.operation/op-name   dyn
                                                                              ::pcp/source-op-name a
                                                                              :com.wsscode.pathom3.connect.planner/expects     {:a {:b {}
                                                                                                                                    :c {}}}
                                                                              :com.wsscode.pathom3.connect.planner/foreign-ast {:children [{:children     [{:dispatch-key :c
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
                                                                              :com.wsscode.pathom3.connect.planner/input       {}
                                                                              :com.wsscode.pathom3.connect.planner/node-id     4}}
               :com.wsscode.pathom3.connect.planner/root                  4})))))

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
           '{:com.wsscode.pathom3.connect.planner/nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name   dyn,
                                                                            ::pcp/source-op-name a
                                                                            :com.wsscode.pathom3.connect.planner/expects     {:a {:b {:b {}},
                                                                                                                                  :c {:c {}}}},
                                                                            :com.wsscode.pathom3.connect.planner/input       {},
                                                                            :com.wsscode.pathom3.connect.planner/node-id     1,
                                                                            :com.wsscode.pathom3.connect.planner/foreign-ast {:type     :root,
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
             :com.wsscode.pathom3.connect.planner/index-ast             {:a {:type         :join,
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
             :com.wsscode.pathom3.connect.planner/index-resolver->nodes {dyn #{1}},
             :com.wsscode.pathom3.connect.planner/index-attrs           {:a #{1}},
             :com.wsscode.pathom3.connect.planner/root                  1}))))

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
             ::pcp/index-resolver->nodes {a #{}}})))

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
             ::pcp/index-resolver->nodes {a #{}
                                          b #{2}}})))

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
             '#:com.wsscode.pathom3.connect.planner{:index-ast             {:a {:dispatch-key :a
                                                                                :key          :a
                                                                                :type         :prop}
                                                                            :b {:dispatch-key :b
                                                                                :key          :b
                                                                                :type         :prop}}
                                                    :index-attrs           {:a #{1}
                                                                            :b #{1}}
                                                    :index-resolver->nodes {ab #{1}}
                                                    :nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    ab
                                                                               :com.wsscode.pathom3.connect.planner/expects      {:a {}
                                                                                                                                  :b {}}
                                                                               :com.wsscode.pathom3.connect.planner/node-id      1
                                                                               :com.wsscode.pathom3.connect.planner/node-parents #{3}
                                                                               :com.wsscode.pathom3.connect.planner/run-next     6}
                                                                            3 #:com.wsscode.pathom3.connect.planner{:node-id 3
                                                                                                                    :run-and #{1}}
                                                                            4 #:com.wsscode.pathom3.connect.planner{:node-parents #{6}}
                                                                            5 #:com.wsscode.pathom3.connect.planner{:node-parents #{6}}
                                                                            6 #:com.wsscode.pathom3.connect.planner{:node-id      6
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
             '#:com.wsscode.pathom3.connect.planner{:index-ast             {:a {:dispatch-key :a
                                                                                :key          :a
                                                                                :type         :prop}
                                                                            :b {:dispatch-key :b
                                                                                :key          :b
                                                                                :type         :prop}}
                                                    :index-attrs           {:a #{1}
                                                                            :b #{1}}
                                                    :index-resolver->nodes {ab #{1}}
                                                    :nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    ab
                                                                               :com.wsscode.pathom3.connect.planner/expects      {:a {}
                                                                                                                                  :b {}}
                                                                               :com.wsscode.pathom3.connect.planner/node-id      1
                                                                               :com.wsscode.pathom3.connect.planner/node-parents #{3}
                                                                               :com.wsscode.pathom3.connect.planner/run-next     5}
                                                                            3 #:com.wsscode.pathom3.connect.planner{:node-id 3
                                                                                                                    :run-and #{1}}
                                                                            5 #:com.wsscode.pathom3.connect.planner{:node-parents #{1}}}
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
             '#:com.wsscode.pathom3.connect.planner{:index-ast             {:a {:dispatch-key :a
                                                                                :key          :a
                                                                                :type         :prop}
                                                                            :b {:dispatch-key :b
                                                                                :key          :b
                                                                                :type         :prop}}
                                                    :index-attrs           {:a #{1}
                                                                            :b #{1}}
                                                    :index-resolver->nodes {ab #{1}}
                                                    :nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    ab
                                                                               :com.wsscode.pathom3.connect.planner/expects      {:a {}
                                                                                                                                  :b {}}
                                                                               :com.wsscode.pathom3.connect.planner/node-id      1
                                                                               :com.wsscode.pathom3.connect.planner/node-parents #{3}
                                                                               :com.wsscode.pathom3.connect.planner/run-next     5}
                                                                            3 #:com.wsscode.pathom3.connect.planner{:node-id 3
                                                                                                                    :run-and #{1}}
                                                                            5 #:com.wsscode.pathom3.connect.planner{:node-id      5
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
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    dynamic-resolver,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:a {},
                                                                                                                                :c {},
                                                                                                                                :b {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {},
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                             :com.wsscode.pathom3.connect.planner/foreign-ast  {:type     :root,
                                                                                                                                :children [{:type         :prop,
                                                                                                                                            :dispatch-key :a,
                                                                                                                                            :key          :a}
                                                                                                                                           {:type         :prop,
                                                                                                                                            :dispatch-key :c,
                                                                                                                                            :key          :c}
                                                                                                                                           {:type         :prop,
                                                                                                                                            :dispatch-key :b,
                                                                                                                                            :key          :b}]},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{3}},
                                                                          4 #:com.wsscode.pathom3.connect.planner{:node-id 4,
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
  (is (= (pcp/optimize-dynamic-resolver-chain
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

  (is (= (pcp/optimize-dynamic-resolver-chain
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

(deftest simplify-branch-test
  (is (= (pcp/simplify-branch-node
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    dynamic-resolver,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:a {},
                                                                                                                                :b {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {},
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                             :com.wsscode.pathom3.connect.planner/foreign-ast  {:type     :root,
                                                                                                                                :children [{:type         :prop,
                                                                                                                                            :dispatch-key :a,
                                                                                                                                            :key          :a}
                                                                                                                                           {:type         :prop,
                                                                                                                                            :dispatch-key :b,
                                                                                                                                            :key          :b}]},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{3}},
                                                                          3 #:com.wsscode.pathom3.connect.planner{:node-id 3,
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
                 :root                  1})))

(deftest transfer-node-parents-test
  (is (= (pcp/transfer-node-parents
           '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    dynamic-resolver,
                                                                             :com.wsscode.pathom3.connect.planner/expects      {:a {},
                                                                                                                                :b {}},
                                                                             :com.wsscode.pathom3.connect.planner/input        {},
                                                                             :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                             :com.wsscode.pathom3.connect.planner/foreign-ast  {:type     :root,
                                                                                                                                :children [{:type         :prop,
                                                                                                                                            :dispatch-key :a,
                                                                                                                                            :key          :a}
                                                                                                                                           {:type         :prop,
                                                                                                                                            :dispatch-key :b,
                                                                                                                                            :key          :b}]},
                                                                             :com.wsscode.pathom3.connect.planner/node-parents #{3}},
                                                                          3 #:com.wsscode.pathom3.connect.planner{:node-id 3,
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
         '#:com.wsscode.pathom3.connect.planner{:nodes                 {1 {:com.wsscode.pathom3.connect.operation/op-name    dynamic-resolver,
                                                                           :com.wsscode.pathom3.connect.planner/expects      {:a {},
                                                                                                                              :b {}},
                                                                           :com.wsscode.pathom3.connect.planner/input        {},
                                                                           :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                           :com.wsscode.pathom3.connect.planner/foreign-ast  {:type     :root,
                                                                                                                              :children [{:type         :prop,
                                                                                                                                          :dispatch-key :a,
                                                                                                                                          :key          :a}
                                                                                                                                         {:type         :prop,
                                                                                                                                          :dispatch-key :b,
                                                                                                                                          :key          :b}]},
                                                                           :com.wsscode.pathom3.connect.planner/node-parents #{3}},
                                                                        3 #:com.wsscode.pathom3.connect.planner{:node-id 3,
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
