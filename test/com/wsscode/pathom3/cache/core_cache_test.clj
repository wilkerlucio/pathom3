(ns com.wsscode.pathom3.cache.core-cache-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.cache :as p.cache]
    [com.wsscode.pathom3.cache.core-cache :as cache]))

(deftest lru-cache-test
  (let [cache* (cache/lru-cache {} 3)]
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
