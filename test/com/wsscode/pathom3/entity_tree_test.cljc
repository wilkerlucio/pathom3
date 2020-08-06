(ns com.wsscode.pathom3.entity-tree-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.entity-tree :as p.e]
    [com.wsscode.pathom3.specs :as p.spec]))

(deftest merge-entity-data-test
  (is (= (p.e/merge-entity-data
           {:foo "bar" :a 1}
           {:buz "baz" :a 2 :b 3})
         {:foo "bar", :a 2, :buz "baz", :b 3})))

(deftest entity-test
  (is (= (p.e/entity {::p.e/cache-tree* (atom {})
                      ::p.spec/path     []})
         {}))

  (testing "get root when path isn't provided"
    (is (= (p.e/entity {::p.e/cache-tree* (atom {:foo "bar"})})
           {:foo "bar"})))

  (is (= (p.e/entity {::p.e/cache-tree* (atom {:foo "bar"})
                      ::p.spec/path     []})
         {:foo "bar"}))

  (is (= (p.e/entity {::p.e/cache-tree* (atom {:foo {:baz "bar"}})
                      ::p.spec/path     [:foo]})
         {:baz "bar"}))

  (is (= (p.e/entity {::p.e/cache-tree* (atom {:foo {:baz "bar"}})
                      ::p.spec/path     [:baz]})
         {}))

  (is (= (p.e/entity {::p.e/cache-tree* (atom {:foo [{:baz "bar"}]})
                      ::p.spec/path      [:foo 0]})
         {:baz "bar"})))

(deftest swap-entity!-test
  (let [tree* (atom {})]
    (is (= (p.e/swap-entity! {::p.e/cache-tree* tree*
                              ::p.spec/path     []}
                             assoc :foo "bar")
           {:foo "bar"}
           @tree*)))

  (let [tree* (atom {:bar {:a 1}})]
    (is (= (p.e/swap-entity! {::p.e/cache-tree* tree*
                              ::p.spec/path     [:bar]}
                             assoc :b 2)
           {:bar {:a 1, :b 2}}
           @tree*)))

  (testing "works with vector positions"
    (let [tree* (atom {:bar [{:a 1}
                             {:a 2}]})]
      (is (= (p.e/swap-entity! {::p.e/cache-tree* tree*
                                ::p.spec/path     [:bar 0]}
                               assoc :b 2)
             {:bar [{:a 1, :b 2} {:a 2}]}
             @tree*)))))
