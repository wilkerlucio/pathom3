(ns com.wsscode.pathom3.format.shape-descriptor-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.format.shape-descriptor :as psd]))

(deftest query->shape-descriptor-test
  (testing "empty query"
    (is (= (psd/query->shape-descriptor
             [])
           {})))

  (testing "single attribute"
    (is (= (psd/query->shape-descriptor
             [:foo])
           {:foo {}})))

  (testing "multiple attributes and nesting"
    (is (= (psd/query->shape-descriptor
             [{:foo [:bar]} :baz])
           {:foo {:bar {}}
            :baz {}})))

  (testing "combining union queries"
    (is (= (psd/query->shape-descriptor
             [{:foo {:a [:x] :b [:y]}}])
           {:foo {:x {} :y {}}}))))
