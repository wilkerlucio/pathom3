(ns com.wsscode.pathom3.connect.operation-test
  (:require
    [clojure.spec.alpha :as s]
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.operation :as pco]))

(deftest resolver-test
  (testing "creating resolvers"
    (let [resolver (pco/resolver 'foo {::pco/output [:foo]} (fn [_ _] {:foo "bar"}))]
      (is (= (resolver nil nil)
             {:foo "bar"}))

      (is (= (pco/operation-config resolver)
             {::pco/name     'foo
              ::pco/input    []
              ::pco/provides {:foo {}}
              ::pco/output   [:foo]}))))

  (testing "creating resolver from pure maps"
    (let [resolver (pco/resolver {::pco/name    'foo
                                  ::pco/output  [:foo]
                                  ::pco/resolve (fn [_ _] "bar")})]
      (is (= (resolver nil nil)
             "bar"))

      (is (= (pco/operation-config resolver)
             {::pco/name     'foo
              ::pco/output   [:foo]
              ::pco/input    []
              ::pco/provides {:foo {}}})))))

(deftest defresolver-syntax-test
  (testing "classic form"
    (is (= (s/conform ::pco/defresolver-args '[foo [env input]
                                               {::pco/output [:foo]}
                                               {:foo "bar"}])
           '{:name    foo
             :arglist [[:sym env] [:sym input]]
             :options {::pco/output [:foo]}
             :body    [{:foo "bar"}]})))

  (testing "short keyword simple output form"
    (is (= (s/conform ::pco/defresolver-args '[foo [env input] :foo "bar"])
           '{:name        foo
             :arglist     [[:sym env] [:sym input]]
             :output-attr :foo
             :body        ["bar"]})))

  (testing "short keyword simple output form + options"
    (is (= (s/conform ::pco/defresolver-args '[foo [env input] :foo {::pco/input [:x]} "bar"])
           '{:name        foo
             :arglist     [[:sym env] [:sym input]]
             :output-attr :foo
             :options     {::pco/input [:x]}
             :body        ["bar"]})))

  (testing "argument destructuring"
    (is (= (s/conform ::pco/operation-argument 'foo)
           '[:sym foo]))

    (is (= (s/conform ::pco/operation-argument '{:keys [foo]})
           '[:map {:keys [foo]}]))

    (is (= (s/conform ::pco/operation-argument '{:keys [foo] :as bar})
           '[:map {:keys [foo] :as bar}]))

    (is (= (s/conform ::pco/operation-argument '{:strs [foo]})
           :clojure.spec.alpha/invalid)))

  (testing "fails without options or output"
    (is (= (s/explain-data ::pco/defresolver-args '[foo [env input] "bar"])
           '#:clojure.spec.alpha{:problems [{:path [],
                                             :pred (clojure.core/fn
                                                     must-have-output-prop-or-options
                                                     [{:keys [output-attr options]}]
                                                     (clojure.core/or output-attr options)),
                                             :val  {:name    foo,
                                                    :arglist [[:sym env] [:sym input]],
                                                    :body    ["bar"]},
                                             :via  [::pco/defresolver-args],
                                             :in   []}],
                                 :spec     ::pco/defresolver-args,
                                 :value    [foo [env input] "bar"]}))))

(deftest extract-destructure-map-keys-as-keywords-test
  (is (= (pco/extract-destructure-map-keys-as-keywords
           '{:keys [foo]})
         [:foo]))

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

  (testing "simple output attr"
    (is (= (pco/params->resolver-options
             '{:name        foo
               :arglist     [[:sym env] [:sym input]]
               :output-attr :foo
               :body        ["bar"]})
           {::pco/output [:foo]})))

  (testing "output attr + options"
    (is (= (pco/params->resolver-options
             '{:name        foo
               :arglist     [[:sym env] [:sym input]]
               :output-attr :foo
               :options     {::pco/input [:x]}
               :body        ["bar"]})
           {::pco/input  [:x]
            ::pco/output [:foo]})))

  (testing "inferred input"
    (is (= (pco/params->resolver-options
             '{:name        foo
               :arglist     [[:sym env] [:map {:keys [dep]}]]
               :output-attr :foo
               :body        ["bar"]})
           {::pco/input  [:dep]
            ::pco/output [:foo]}))

    (testing "preserve user input when defined"
      (is (= (pco/params->resolver-options
               '{:name        foo
                 :arglist     [[:sym env] [:map {:keys [dep]}]]
                 :options     {::pco/input [:dep :other]}
                 :output-attr :foo
                 :body        ["bar"]})
             {::pco/input  [:dep :other]
              ::pco/output [:foo]})))))

(deftest normalize-arglist-test
  (is (= (pco/normalize-arglist [])
         '[[:sym _] [:sym _]]))

  (is (= (pco/normalize-arglist '[[:sym input]])
         '[[:sym _] [:sym input]]))

  (is (= (pco/normalize-arglist '[[:sym env] [:sym input]])
         '[[:sym env] [:sym input]])))

(deftest defresolver-test
  (testing "single attribute resolver, no args capture"
    (is (= (macroexpand-1
             `(pco/defresolver ~'foo ~'[] :sample "bar"))
           '(def foo
              (com.wsscode.pathom3.connect.operation/resolver
                user/foo
                #:com.wsscode.pathom3.connect.operation{:output [:sample]}
                (clojure.core/fn [_ _] "bar"))))))

  (testing "explicit output, no args"
    (is (= (macroexpand-1
             `(pco/defresolver ~'foo ~'[] {::pco/output [:foo]} {:foo "bar"}))
           '(def foo
              (com.wsscode.pathom3.connect.operation/resolver
                user/foo
                #:com.wsscode.pathom3.connect.operation{:output [:foo]}
                (clojure.core/fn [_ _] {:foo "bar"}))))))

  (testing "single attribute, including implicit import via destructuring"
    (is (= (macroexpand-1
             `(pco/defresolver ~'foo ~'[{:keys [dep]}] :sample "bar"))
           '(def foo
              (com.wsscode.pathom3.connect.operation/resolver
                user/foo
                #:com.wsscode.pathom3.connect.operation{:output [:sample],
                                                        :input  [:dep]}
                (clojure.core/fn [_ {:keys [dep]}] "bar")))))))
