(ns com.wsscode.pathom3.connect.operation-test
  (:require
    #?(:clj [clojure.spec.alpha :as s])
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.operation :as pco]))

(deftest resolver-test
  (testing "creating resolvers"
    (let [resolver (pco/resolver 'foo {::pco/output [:foo]} (fn [_ _] {:foo "bar"}))]
      (is (= (resolver)
             {:foo "bar"}))

      (is (= (pco/operation-config resolver)
             {::pco/op-name  'foo
              ::pco/input    []
              ::pco/provides {:foo {}}
              ::pco/output   [:foo]}))))

  (testing "creating resolver from pure maps"
    (let [resolver (pco/resolver {::pco/op-name 'foo
                                  ::pco/output  [:foo]
                                  ::pco/resolve (fn [_ _] "bar")})]
      (is (= (resolver nil nil)
             "bar"))

      (is (= (pco/operation-config resolver)
             {::pco/op-name  'foo
              ::pco/output   [:foo]
              ::pco/input    []
              ::pco/provides {:foo {}}}))))

  (testing "validates configuration map"
    (try
      (pco/resolver 'foo {::pco/input #{:invalid}} (fn [_ _] {:sample "bar"}))
      (catch #?(:clj Throwable :cljs :default) e
        (is (= (-> e
                   (ex-data)
                   #?(:clj  (update :explain-data dissoc :clojure.spec.alpha/spec :clojure.spec.alpha/value)
                      :cljs (update :explain-data dissoc :cljs.spec.alpha/spec :cljs.spec.alpha/value)))
               {:explain-data {#?(:clj  :clojure.spec.alpha/problems
                                  :cljs :cljs.spec.alpha/problems) [{:in   [:com.wsscode.pathom3.connect.operation/input]
                                                                     :path [:com.wsscode.pathom3.connect.operation/input]
                                                                     :pred #?(:clj  'clojure.core/vector?
                                                                              :cljs 'cljs.core/vector?)
                                                                     :val  #{:invalid}
                                                                     :via  [:com.wsscode.pathom3.connect.operation/input]}]}})))))

  (testing "name config overrides syntax name"
    (let [resolver (pco/resolver 'foo {::pco/op-name 'bar
                                       ::pco/resolve (fn [_ _] {})}
                     (fn [_ _] {:foo "bar"}))]
      (is (= (pco/operation-config resolver)
             {::pco/op-name 'bar}))))

  (testing "dynamic resolver"
    (let [resolver (pco/resolver {::pco/op-name           'foo
                                  ::pco/dynamic-resolver? true
                                  ::pco/resolve           (fn [_ _] "bar")})]

      (is (= (pco/operation-config resolver)
             {::pco/op-name           'foo
              ::pco/dynamic-resolver? true}))))

  (testing "transform"
    (let [resolver (pco/resolver 'foo {::pco/output    [:foo]
                                       ::pco/transform (fn [config]
                                                         (assoc config ::other "bar"))}
                     (fn [_ _] {:foo "bar"}))]
      (is (= (pco/operation-config resolver)
             {::pco/op-name  'foo
              ::other        "bar"
              ::pco/output   [:foo]
              ::pco/input    []
              ::pco/provides {:foo {}}}))))

  (testing "noop when called with a resolver"
    (let [resolver (-> {::pco/op-name 'foo
                        ::pco/output  [:foo]
                        ::pco/resolve (fn [_ _] "bar")}
                       (pco/resolver)
                       (pco/resolver)
                       (pco/resolver))]
      (is (= (resolver nil nil)
             "bar"))

      (is (= (pco/operation-config resolver)
             {::pco/op-name  'foo
              ::pco/output   [:foo]
              ::pco/input    []
              ::pco/provides {:foo {}}})))))

(deftest mutation-test
  (testing "creating mutations"
    (let [mutation (pco/mutation 'foo {::pco/output [:foo]} (fn [_ _] {:foo "bar"}))]
      (is (= (mutation)
             {:foo "bar"}))

      (is (= (pco/operation-config mutation)
             {::pco/op-name  'foo
              ::pco/provides {:foo {}}
              ::pco/output   [:foo]}))))

  (testing "creating mutation from pure maps"
    (let [mutation (pco/mutation {::pco/op-name 'foo
                                  ::pco/output  [:foo]
                                  ::pco/mutate  (fn [_ _] {:foo "bar"})})]
      (is (= (mutation nil nil)
             {:foo "bar"}))

      (is (= (pco/operation-config mutation)
             {::pco/op-name  'foo
              ::pco/output   [:foo]
              ::pco/provides {:foo {}}}))))

  (testing "validates configuration map"
    (try
      (pco/mutation 'foo {::pco/input #{:invalid}} (fn [_ _] {:sample "bar"}))
      (catch #?(:clj Throwable :cljs :default) e
        (is (= (-> e
                   (ex-data)
                   #?(:clj  (update :explain-data dissoc :clojure.spec.alpha/spec :clojure.spec.alpha/value)
                      :cljs (update :explain-data dissoc :cljs.spec.alpha/spec :cljs.spec.alpha/value)))
               {:explain-data {#?(:clj  :clojure.spec.alpha/problems
                                  :cljs :cljs.spec.alpha/problems) [{:in   [:com.wsscode.pathom3.connect.operation/input]
                                                                     :path [:com.wsscode.pathom3.connect.operation/input]
                                                                     :pred #?(:clj  'clojure.core/vector?
                                                                              :cljs 'cljs.core/vector?)
                                                                     :val  #{:invalid}
                                                                     :via  [:com.wsscode.pathom3.connect.operation/input]}]}})))))

  (testing "name config overrides syntax name"
    (let [mutation (pco/mutation 'foo {::pco/op-name 'bar
                                       ::pco/mutate  (fn [_ _] {})}
                                 (fn [_ _] {:foo "bar"}))]
      (is (= (pco/operation-config mutation)
             {::pco/op-name 'bar}))))

  (testing "transform"
    (let [mutation (pco/mutation 'foo {::pco/output    [:foo]
                                       ::pco/transform (fn [config]
                                                         (assoc config ::other "bar"))}
                                 (fn [_ _] {:foo "bar"}))]
      (is (= (pco/operation-config mutation)
             {::pco/op-name  'foo
              ::other        "bar"
              ::pco/output   [:foo]
              ::pco/provides {:foo {}}}))))

  (testing "noop when called with a mutation"
    (let [mutation (-> {::pco/op-name 'foo
                        ::pco/output  [:foo]
                        ::pco/mutate  (fn [_ _] {:data "bar"})}
                       (pco/mutation)
                       (pco/mutation)
                       (pco/mutation))]
      (is (= (mutation nil nil)
             {:data "bar"}))

      (is (= (pco/operation-config mutation)
             {::pco/op-name  'foo
              ::pco/output   [:foo]
              ::pco/provides {:foo {}}})))))

#?(:clj
   (deftest defresolver-syntax-test
     (testing "classic form"
       (is (= (s/conform ::pco/defresolver-args '[foo [env input]
                                                  {::pco/output [:foo]}
                                                  {:foo "bar"}])
              '{:name    foo
                :arglist [[:sym env] [:sym input]]
                :options {::pco/output [:foo]}
                :body    [{:foo "bar"}]})))

     (testing "visible output shape"
       (is (= (s/conform ::pco/defresolver-args '[foo [env input] {:foo "bar"}])
              '{:name    foo
                :arglist [[:sym env] [:sym input]]
                :body    [{:foo "bar"}]})))

     (testing "argument destructuring"
       (is (= (s/conform ::pco/operation-argument 'foo)
              '[:sym foo]))

       (is (= (s/conform ::pco/operation-argument '{:keys [foo]})
              '[:map {:keys [foo]}]))

       (is (= (s/conform ::pco/operation-argument '{:keys [foo] :as bar})
              '[:map {:keys [foo] :as bar}]))

       (is (= (s/conform ::pco/operation-argument '{:strs [foo]})
              :clojure.spec.alpha/invalid))

       (testing "keywords on keys"
         (is (= (s/conform ::pco/operation-argument '{:keys [:foo]})
                '[:map {:keys [:foo]}]))

         (is (= (s/conform ::pco/operation-argument '{:keys [:foo/bar]})
                '[:map {:keys [:foo/bar]}]))))

     (testing "fails without options or visible map"
       (is (= (s/explain-data ::pco/defresolver-args '[foo [env input] "bar"])
              '#:clojure.spec.alpha{:problems [{:path [],
                                                :pred (clojure.core/fn
                                                        must-have-output-visible-map-or-options
                                                        [{:keys [body options]}]
                                                        (clojure.core/or (clojure.core/map? (clojure.core/last body)) options)),
                                                :val  {:name    foo,
                                                       :arglist [[:sym env] [:sym input]],
                                                       :body    ["bar"]},
                                                :via  [::pco/defresolver-args],
                                                :in   []}],
                                    :spec     ::pco/defresolver-args,
                                    :value    [foo [env input] "bar"]})))))

(deftest extract-destructure-map-keys-as-keywords-test
  (is (= (pco/extract-destructure-map-keys-as-keywords
           '{:keys [foo]})
         [:foo]))

  (is (= (pco/extract-destructure-map-keys-as-keywords
           '{:keys [:foo]})
         [:foo]))

  (is (= (pco/extract-destructure-map-keys-as-keywords
           '{:keys [:foo/bar]})
         [:foo/bar]))

  (is (= (pco/extract-destructure-map-keys-as-keywords
           '{:keys      [foo]
             :user/keys [id name]
             :as        user})
         [:foo :user/id :user/name]))

  (is (= (pco/extract-destructure-map-keys-as-keywords
           '{renamed :foo})
         [:foo]))

  (is (= (pco/extract-destructure-map-keys-as-keywords
           '{{deep :value} :foo})
         [:foo]))

  (is (= (pco/extract-destructure-map-keys-as-keywords
           '{{:keys [value]} :foo})
         [:foo])))

(deftest params->resolver-options-test
  (testing "classic case"
    (is (= (pco/params->resolver-options
             '{:name    foo
               :arglist [[:name env] [:name input]]
               :options {::pco/output [:foo]}
               :body    [{:foo "bar"}]})
           {::pco/output [:foo]})))

  (testing "inferred input"
    (is (= (pco/params->resolver-options
             '{:name    foo
               :arglist [[:sym env] [:map {:keys [dep]}]]
               :body    [{:foo "bar"}]})
           {::pco/input  [:dep]
            ::pco/output [:foo]}))

    (testing "preserve user input when defined"
      (is (= (pco/params->resolver-options
               '{:name    foo
                 :arglist [[:sym env] [:map {:keys [dep]}]]
                 :options {::pco/input [:dep :other]}
                 :body    [{:foo "bar"}]})
             {::pco/input  [:dep :other]
              ::pco/output [:foo]}))))

  (testing "implicit output"
    (is (= (pco/params->resolver-options
             '{:name    foo
               :arglist [[:sym env] [:sym input]]
               :body    [nil {:foo "bar"}]})
           {::pco/output [:foo]}))

    (testing "nested body"
      (is (= (pco/params->resolver-options
               '{:name    foo
                 :arglist [[:sym env] [:sym input]]
                 :body    [{:foo  "bar"
                            :buz  "baz"
                            :deep {:nested (call-something)}
                            :seq  [{:with "things"} {:around "here "}]}]})
             {::pco/output [:buz
                            {:deep [:nested]}
                            :foo
                            {:seq [:with :around]}]})))

    (testing "preserve user output when defined"
      (is (= (pco/params->resolver-options
               '{:name    foo
                 :arglist [[:sym env] [:sym input]]
                 :options {::pco/output [:foo :bar]}
                 :body    [{:foo "bar"}]})
             {::pco/output [:foo :bar]})))))

(deftest params->mutation-options-test
  (testing "classic case"
    (is (= (pco/params->mutation-options
             '{:name    foo
               :arglist [[:name env] [:name input]]
               :options {::pco/output [:foo]}
               :body    [{:foo "bar"}]})
           {::pco/output [:foo]})))

  (testing "inferred params"
    (is (= (pco/params->mutation-options
             '{:name    foo
               :arglist [[:sym env] [:map {:keys [dep]}]]
               :body    [{:foo "bar"}]})
           {::pco/params [:dep]
            ::pco/output [:foo]}))

    (testing "preserve user params when defined"
      (is (= (pco/params->mutation-options
               '{:name    foo
                 :arglist [[:sym env] [:map {:keys [dep]}]]
                 :options {::pco/params [:dep :other]}
                 :body    [{:foo "bar"}]})
             {::pco/params [:dep :other]
              ::pco/output [:foo]}))))

  (testing "implicit output"
    (is (= (pco/params->mutation-options
             '{:name    foo
               :arglist [[:sym env] [:sym params]]
               :body    [nil {:foo "bar"}]})
           {::pco/output [:foo]}))

    (testing "nested body"
      (is (= (pco/params->mutation-options
               '{:name    foo
                 :arglist [[:sym env] [:sym params]]
                 :body    [{:foo  "bar"
                            :buz  "baz"
                            :deep {:nested (call-something)}
                            :seq  [{:with "things"} {:around "here "}]}]})
             {::pco/output [:buz
                            {:deep [:nested]}
                            :foo
                            {:seq [:with :around]}]})))

    (testing "preserve user output when defined"
      (is (= (pco/params->mutation-options
               '{:name    foo
                 :arglist [[:sym env] [:sym params]]
                 :options {::pco/output [:foo :bar]}
                 :body    [{:foo "bar"}]})
             {::pco/output [:foo :bar]})))))

(deftest normalize-arglist-test
  (is (= (pco/normalize-arglist [])
         '[[:sym _] [:sym _]]))

  (is (= (pco/normalize-arglist '[[:sym input]])
         '[[:sym _] [:sym input]]))

  (is (= (pco/normalize-arglist '[[:sym env] [:sym input]])
         '[[:sym env] [:sym input]])))

#?(:clj
   (deftest defresolver-test
     (testing "docstring"
       (is (= (macroexpand-1
                `(pco/defresolver ~'foo "documentation" ~'[] {:sample "bar"}))
              '(def foo
                 "documentation"
                 (com.wsscode.pathom3.connect.operation/resolver
                   'user/foo
                   #:com.wsscode.pathom3.connect.operation{:output [:sample]}
                   (clojure.core/fn foo [_ _] {:sample "bar"}))))))

     (testing "validates configuration map"
       (try
         (macroexpand-1
           `(pco/defresolver ~'foo ~'[] {::pco/input #{:invalid}} {:sample "bar"}))
         (catch #?(:clj Throwable :cljs :default) e
           (is (= (-> (ex-cause e)
                      (ex-data)
                      (update :explain-data dissoc :clojure.spec.alpha/spec))
                  {:explain-data #:clojure.spec.alpha{:problems [{:in   [:com.wsscode.pathom3.connect.operation/input]
                                                                  :path [:com.wsscode.pathom3.connect.operation/input]
                                                                  :pred 'clojure.core/vector?
                                                                  :val  #{:invalid}
                                                                  :via  [:com.wsscode.pathom3.connect.operation/input]}]
                                                      :value    #:com.wsscode.pathom3.connect.operation{:input #{:invalid}}}})))))

     (testing "implicit output resolver, no args capture"
       (is (= (macroexpand-1
                `(pco/defresolver ~'foo ~'[] {:sample "bar"}))
              '(def foo
                 (com.wsscode.pathom3.connect.operation/resolver
                   'user/foo
                   #:com.wsscode.pathom3.connect.operation{:output [:sample]}
                   (clojure.core/fn foo [_ _] {:sample "bar"}))))))

     (testing "explicit output, no args"
       (is (= (macroexpand-1
                `(pco/defresolver ~'foo ~'[] {::pco/output [:foo]} {:foo "bar"}))
              '(def foo
                 (com.wsscode.pathom3.connect.operation/resolver
                   'user/foo
                   #:com.wsscode.pathom3.connect.operation{:output [:foo]}
                   (clojure.core/fn foo [_ _] {:foo "bar"}))))))

     (testing "implicit output, including implicit inputs via destructuring"
       (is (= (macroexpand-1
                `(pco/defresolver ~'foo ~'[{:keys [dep]}] {:sample "bar"}))
              '(def foo
                 (com.wsscode.pathom3.connect.operation/resolver
                   'user/foo
                   #:com.wsscode.pathom3.connect.operation{:output [:sample],
                                                           :input  [:dep]}
                   (clojure.core/fn foo [_ {:keys [dep]}] {:sample "bar"}))))))

     (testing "implicit output, including implicit inputs via destructuring"
       (is (= (macroexpand-1
                `(pco/defresolver ~'foo ~'[{:keys [dep]}] {::pco/output [{:sample [:thing]}]} {:sample "bar"}))
              '(def foo
                 (com.wsscode.pathom3.connect.operation/resolver
                   'user/foo
                   #:com.wsscode.pathom3.connect.operation{:output [{:sample [:thing]}],
                                                           :input  [:dep]}
                   (clojure.core/fn foo [_ {:keys [dep]}] {:sample "bar"}))))))

     (testing "unform options to retain data"
       (s/def ::or-thing (s/or :foo int? :bar string?))

       (is (= (macroexpand-1
                `(pco/defresolver ~'foo ~'[] {::or-thing 3} {:sample "bar"}))
              '(def foo
                 (com.wsscode.pathom3.connect.operation/resolver
                   'user/foo
                   {::pco/output [:sample]
                    ::or-thing   3}
                   (clojure.core/fn foo [_ _] {:sample "bar"}))))))))

#?(:clj
   (deftest defmutation-test
     (testing "docstring"
       (is (= (macroexpand-1
                `(pco/defmutation ~'foo "documentation" ~'[] {:sample "bar"}))
              '(def foo
                 "documentation"
                 (com.wsscode.pathom3.connect.operation/mutation
                   'user/foo
                   #:com.wsscode.pathom3.connect.operation{:output [:sample]}
                   (clojure.core/fn foo [_ _] {:sample "bar"}))))))

     (testing "implicit output resolver, no args capture"
       (is (= (macroexpand-1
                `(pco/defmutation ~'foo ~'[] {:sample "bar"}))
              '(def foo
                 (com.wsscode.pathom3.connect.operation/mutation
                   'user/foo
                   #:com.wsscode.pathom3.connect.operation{:output [:sample]}
                   (clojure.core/fn foo [_ _] {:sample "bar"}))))))

     (testing "explicit output, no args"
       (is (= (macroexpand-1
                `(pco/defmutation ~'foo ~'[] {::pco/output [:foo]} {:foo "bar"}))
              '(def foo
                 (com.wsscode.pathom3.connect.operation/mutation
                   'user/foo
                   #:com.wsscode.pathom3.connect.operation{:output [:foo]}
                   (clojure.core/fn foo [_ _] {:foo "bar"}))))))

     (testing "implicit output, including implicit params via destructuring"
       (is (= (macroexpand-1
                `(pco/defmutation ~'foo ~'[{:keys [dep]}] {:sample "bar"}))
              '(def foo
                 (com.wsscode.pathom3.connect.operation/mutation
                   'user/foo
                   #:com.wsscode.pathom3.connect.operation{:output [:sample],
                                                           :params  [:dep]}
                   (clojure.core/fn foo [_ {:keys [dep]}] {:sample "bar"}))))))

     (testing "implicit output, including implicit params via destructuring"
       (is (= (macroexpand-1
                `(pco/defmutation ~'foo ~'[{:keys [dep]}] {::pco/output [{:sample [:thing]}]} {:sample "bar"}))
              '(def foo
                 (com.wsscode.pathom3.connect.operation/mutation
                   'user/foo
                   #:com.wsscode.pathom3.connect.operation{:output [{:sample [:thing]}],
                                                           :params  [:dep]}
                   (clojure.core/fn foo [_ {:keys [dep]}] {:sample "bar"}))))))

     (testing "unform options to retain data"
       (s/def ::or-thing (s/or :foo int? :bar string?))

       (is (= (macroexpand-1
                `(pco/defmutation ~'foo ~'[] {::or-thing 3} {:sample "bar"}))
              '(def foo
                 (com.wsscode.pathom3.connect.operation/mutation
                   'user/foo
                   {::pco/output [:sample]
                    ::or-thing   3}
                   (clojure.core/fn foo [_ _] {:sample "bar"}))))))))

(deftest with-node-params-test
  (is (= (pco/with-node-params {:foo "bar"})
         {:com.wsscode.pathom3.connect.planner/node
          {:com.wsscode.pathom3.connect.planner/params
           {:foo "bar"}}}))

  (is (= (pco/with-node-params {:env "data"} {:foo "bar"})
         {:env
          "data"
          :com.wsscode.pathom3.connect.planner/node
          {:com.wsscode.pathom3.connect.planner/params
           {:foo "bar"}}})))

(deftest params-test
  (is (= (pco/params {})
         {}))
  (is (= (pco/params (pco/with-node-params {:foo "bar"}))
         {:foo "bar"})))

(deftest update-config-test
  (is (= (-> (pbir/constantly-resolver :foo "bar")
             (pco/update-config assoc :extra "data")
             (pco/operation-config))
         '{:com.wsscode.pathom3.connect.operation/input    [],
           :com.wsscode.pathom3.connect.operation/provides {:foo {}},
           :com.wsscode.pathom3.connect.operation/output   [:foo],
           :com.wsscode.pathom3.connect.operation/cache?   false,
           :com.wsscode.pathom3.connect.operation/op-name  foo-constant,
           :extra                                          "data"})))
