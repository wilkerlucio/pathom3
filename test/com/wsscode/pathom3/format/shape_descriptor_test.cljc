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

(deftest shape-descriptor->ast-test
  (testing "empty query"
    (is (= (psd/shape-descriptor->ast
             {})
           {:children []
            :type     :root})))

  (testing "single attribute"
    (is (= (psd/shape-descriptor->ast
             {:foo {}})
           {:type :root, :children [{:type :prop, :dispatch-key :foo, :key :foo}]})))

  (testing "multiple attributes and nesting"
    (is (= (psd/shape-descriptor->ast
             {:foo {:bar {}}
              :baz {}})
           {:type     :root,
            :children [{:type         :join,
                        :dispatch-key :foo,
                        :key          :foo,
                        :children     [{:type :prop, :dispatch-key :bar, :key :bar}]}
                       {:type :prop, :dispatch-key :baz, :key :baz}]}))))

(deftest shape-descriptor->query-test
  (testing "empty query"
    (is (= (psd/shape-descriptor->query
             {})
           [])))

  (testing "single attribute"
    (is (= (psd/shape-descriptor->query
             {:foo {}})
           [:foo])))

  (testing "multiple attributes and nesting"
    (is (= (psd/shape-descriptor->query
             {:foo {:bar {}}
              :baz {}})
           [{:foo [:bar]} :baz]))))

(deftest data->shape-descriptor-test
  (is (= (psd/data->shape-descriptor {})
         {}))

  (is (= (psd/data->shape-descriptor {:foo "bar"})
         {:foo {}}))

  (is (= (psd/data->shape-descriptor {:foo {:bar "baz"}})
         {:foo {:bar {}}}))

  (is (= (psd/data->shape-descriptor {:foo [{:bar "x"}
                                            {:baz "a"}]})
         {:foo {:bar {}
                :baz {}}})))

(deftest missing-test
  (is (= (psd/missing {} {})
         nil))

  (is (= (psd/missing {} {:foo {}})
         {:foo {}}))

  (is (= (psd/missing {} {:foo {:bar {}}})
         {:foo {:bar {}}}))

  (is (= (psd/missing {:foo {}} {})
         nil))

  (is (= (psd/missing {:foo {}} {:foo {}})
         nil))

  (is (= (psd/missing {:foo {}} {:foo {} :bar {}})
         {:bar {}}))

  (is (= (psd/missing {:foo {:bar {}}} {:foo {:bar {}}})
         nil))

  (is (= (psd/missing {:foo {}} {:foo {:bar {}}})
         {:foo {:bar {}}}))

  (is (= (psd/missing {:foo {:bar {}}} {:foo {:bar {} :baz {}}})
         {:foo {:baz {}}})))
