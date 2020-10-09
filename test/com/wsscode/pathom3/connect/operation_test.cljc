(ns com.wsscode.pathom3.connect.operation-test
  (:require
    #?(:clj [clojure.spec.alpha :as s])
    [clojure.test :refer [deftest is are run-tests testing]]
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
         [:foo :user/id :user/name])))

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
             {::pco/output [:foo
                            :buz
                            {:deep [:nested]}
                            {:seq [:with :around]}]})))

    (testing "preserve user output when defined"
      (is (= (pco/params->resolver-options
               '{:name    foo
                 :arglist [[:sym env] [:sym input]]
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

     (testing "implicit output, including implicit import via destructuring"
       (is (= (macroexpand-1
                `(pco/defresolver ~'foo ~'[{:keys [dep]}] {:sample "bar"}))
              '(def foo
                 (com.wsscode.pathom3.connect.operation/resolver
                   'user/foo
                   #:com.wsscode.pathom3.connect.operation{:output [:sample],
                                                           :input  [:dep]}
                   (clojure.core/fn foo [_ {:keys [dep]}] {:sample "bar"}))))))

     (testing "implicit output, including implicit import via destructuring"
       (is (= (macroexpand-1
                `(pco/defresolver ~'foo ~'[{:keys [dep]}] {::pco/output [{:sample [:thing]}]} {:sample "bar"}))
              '(def foo
                 (com.wsscode.pathom3.connect.operation/resolver
                   'user/foo
                   #:com.wsscode.pathom3.connect.operation{:output [{:sample [:thing]}],
                                                           :input  [:dep]}
                   (clojure.core/fn foo [_ {:keys [dep]}] {:sample "bar"}))))))))
