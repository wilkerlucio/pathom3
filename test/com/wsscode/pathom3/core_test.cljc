(ns com.wsscode.pathom3.core-test
  (:require [clojure.test :refer [deftest is are run-tests testing]]
            [com.wsscode.pathom3.core :as p]
            [com.wsscode.pathom3.interface.eql :as p.eql]
            [com.wsscode.pathom3.interface.smart-map :as p.sm]
            [com.wsscode.pathom3.connect.indexes :as pci]
            [clojure.spec.alpha :as s]
            [clojure.spec.alpha :as s]))

(p/defresolver foo [env {:keys [foo]}]
  {::pci/input  [:foo]
   ::pci/output [:bar]}
  {:bar (+ foo 3)})

(p/defresolver foo [{:keys [foo]}] :bar
  (+ foo 3))

(p/defresolver foo [{:keys [foo]}] :bar
  {::p/transform wrap-thing}
  (+ foo 3))

(p/resolver 'foo {::p/output-prop :bar}
  (fn []))

(p/defresolver foo [{:keys [foo]}]
  {::pci/output  [:bar]
   ::p/transform wrap-thing}
  {:bar (+ foo 3)})

(p/defresolver foo [env _] :bar
  {:bar (+ foo 3)})

(p/defresolver foo [] :pi 3.14)
(p/defresolver foo [_] :pi 3.14)

;(def env (p/init-env {::p/registry [foo]}))
;
;(p.eql/process env [{[:foo 5] [:bar]}]) ; => 8
;
;(-> (p.sm/smart-map env {:foo 5})
;    :bar) ; => 8
;
;;; for this example imagine we have a lot of geometric resolvers available already
;(let [magic-geo (p.js/js-proxy geometric-ops-env {::g/left 0 ::g/width 50
;                                                  ::g/top  10 ::g/height 50})]
;  (dom/svg
;    (dom/rect magic-geo) ; works, convert :left to x`
;    (dom/circle magic-geo)) ; works anyway, can figure the cx 25 and cy 30
;  )

(deftest defresolver-syntax-test
  (testing "classic form"
    (is (= (s/conform ::p/defresolver-args '[foo [env input]
                                             {::p/output [:foo]}
                                             {:foo "bar"}])
           '{:sym     foo
             :arglist [[:sym env] [:sym input]]
             :options #:com.wsscode.pathom3.core{:output [:foo]}
             :body    [{:foo "bar"}]})))

  (testing "short keyword simple output form"
    (is (= (s/conform ::p/defresolver-args '[foo [env input] :foo "bar"])
           '{:sym         foo
             :arglist     [[:sym env] [:sym input]]
             :output-prop :foo
             :body        ["bar"]})))

  (testing "short keyword simple output form + options"
    (is (= (s/conform ::p/defresolver-args '[foo [env input] :foo {::p/input [:x]} "bar"])
           '{:sym         foo
             :arglist     [[:sym env] [:sym input]]
             :output-prop :foo
             :options     {::p/input [:x]}
             :body        ["bar"]})))

  (testing "argument destructuring"
    (is (= (s/conform ::p/arg-destructure 'foo)
           '[:sym foo]))

    (is (= (s/conform ::p/arg-destructure '{:keys [foo]})
           '[:map {:keys [foo]}]))

    (is (= (s/conform ::p/arg-destructure '{:keys [foo] :as bar})
           '[:map {:keys [foo] :as bar}]))

    (is (= (s/conform ::p/arg-destructure '{:strs [foo]})
           :clojure.spec.alpha/invalid)))

  (testing "fails without options or output"
    (is (= (s/explain-data ::p/defresolver-args '[foo [env input] "bar"])
           '#:clojure.spec.alpha{:problems [{:path [],
                                             :pred (clojure.core/fn
                                                     must-have-output-prop-or-options
                                                     [{:keys [output-prop options]}]
                                                     (clojure.core/or output-prop options)),
                                             :val {:sym foo,
                                                   :arglist [[:sym env] [:sym input]],
                                                   :body ["bar"]},
                                             :via [:com.wsscode.pathom3.core/defresolver-args],
                                             :in []}],
                                 :spec :com.wsscode.pathom3.core/defresolver-args,
                                 :value [foo [env input] "bar"]}))))
