(ns com.wsscode.pathom3.cache-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.cache :as p.cache]))

(deftest cached-test
  (testing "no cache, always run"
    (is (= (p.cache/cached :foo {} :key #(-> "bar"))
           "bar")))

  (testing "adds to the cache"
    (let [cache* (atom {})]
      (is (= (p.cache/cached :foo {:foo cache*} :key #(-> "bar"))
             "bar"))
      (is (= @cache* {:key "bar"}))))

  (testing "gets from cache"
    (let [cache* (atom {:key "other"})]
      (is (= (p.cache/cached :foo {:foo cache*} :key #(-> "bar"))
             "other"))
      (is (= @cache* {:key "other"})))))

(deftest lru-cache-test
  (let [cache* (p.cache/lru-cache {} 3)]
    (p.cache/-cache-lookup-or-miss cache* :a (constantly 1))
    (p.cache/-cache-lookup-or-miss cache* :b (constantly 2))
    (p.cache/-cache-lookup-or-miss cache* :c (constantly 3))
    (is (= (-> cache* :cache* deref)
           {:a 1 :b 2 :c 3}))
    (p.cache/-cache-lookup-or-miss cache* :c (constantly 5))
    (is (= (-> cache* :cache* deref)
           {:a 1 :b 2 :c 3}))
    (p.cache/-cache-lookup-or-miss cache* :d (constantly 6))
    (is (= (-> cache* :cache* deref)
           {:d 6 :b 2 :c 3}))))
