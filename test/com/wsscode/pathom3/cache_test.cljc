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
