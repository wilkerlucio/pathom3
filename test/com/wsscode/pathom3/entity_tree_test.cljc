(ns com.wsscode.pathom3.entity-tree-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.entity-tree :as p.e]))

(deftest merge-entity-data-test
  (is (= (p.e/merge-entity-data
           {:foo "bar" :a 1}
           {:buz "baz" :a 2 :b 3})
         {:foo "bar", :a 2, :buz "baz", :b 3})))

(deftest with-cache-tree-test
  (is (= @(::p.e/entity-tree* (p.e/with-entity {} {:foo "bar"}))
         {:foo "bar"})))

(deftest swap-entity!-test
  (let [tree* (atom {})]
    (is (= (p.e/swap-entity! {::p.e/entity-tree* tree*}
                             assoc :foo "bar")
           {:foo "bar"}
           @tree*)))

  (let [tree* (atom {:a 1})]
    (is (= (p.e/swap-entity! {::p.e/entity-tree* tree*} assoc :b 2)
           {:a 1, :b 2}
           @tree*))))
