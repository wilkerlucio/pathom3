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

(deftest relax-empty-collections-test
  (is (= (psd/relax-empty-collections
           {:foo {:bar {}}} {:foo []})
         {:foo {}}))

  (is (= (psd/relax-empty-collections
           {:foo {:bar {:baz {}}}}
           {:foo {:bar []}})
         {:foo {:bar {}}}))

  (is (= (psd/relax-empty-collections
           {:foo {:bar {:baz {}}}}
           {:foo [{:bar []}]})
         {:foo {:bar {}}})))

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
         {:foo {:baz {}}}))

  (testing "with data"
    (is (= (psd/missing {:foo {}} {:foo {:bar {}}} {:foo []})
           nil))

    (is (= (psd/missing {:foo {:bar {}}} {:foo {:bar {:baz {}}}}
                        {:foo {:bar []}})
           nil))))

(deftest difference-test
  (is (= (psd/difference {} {})
         {}))

  (is (= (psd/difference {:a {}} {})
         {:a {}}))

  (is (= (psd/difference {:a {}} {:b {}})
         {:a {}}))

  (is (= (psd/difference {:a {}} {:a {}})
         {}))

  (testing "nested"
    (is (= (psd/difference {:a {:b {}}} {:a {}})
           {}))

    (is (= (psd/difference {:a {}} {:a {:b {}}})
           {}))

    (is (= (psd/difference {:a {:b {}}} {:a {:b {}}})
           {}))

    (is (= (psd/difference {:a {:b {}}} {:a {:c {}}})
           {:a {:b {}}}))

    (is (= (psd/difference {:a {:b {} :c {}}} {:a {:b {}}})
           {:a {:c {}}}))))

(deftest select-shape-test
  (is (= (psd/select-shape {} {})
         {}))

  (is (= (psd/select-shape {:foo "bar"} {})
         {}))

  (is (= (psd/select-shape {:foo "bar"} {:foo {}})
         {:foo "bar"}))

  (is (= (psd/select-shape {:foo "bar"} {:foo {} :baz {}})
         {:foo "bar"}))

  (is (= (psd/select-shape {:foo "bar" :baz "baz"} {:foo {}})
         {:foo "bar"}))

  (is (= (psd/select-shape {:foo {:a 1 :b 2}} {:foo {}})
         {:foo {:a 1 :b 2}}))

  (is (= (psd/select-shape {:foo {:a 1 :b 2}} {:foo {:a {}}})
         {:foo {:a 1}}))

  (is (= (psd/select-shape {:foo [{:a 1 :b 2}
                                  {:a 3 :b 4}]} {:foo {:a {}}})
         {:foo [{:a 1}
                {:a 3}]}))

  (is (= (psd/select-shape {:foo [{:a 1 :b 2}
                                  {:b 4}]} {:foo {:a {}}})
         {:foo [{:a 1}
                {}]}))

  (is (= (psd/select-shape {:foo #{{:a 1 :b 2}
                                   {:a 3 :b 4}}} {:foo {:a {}}})
         {:foo #{{:a 1}
                 {:a 3}}}))

  (is (= (psd/select-shape {:foo #{{:a 1 :b 2}
                                   {:b 4}}} {:foo {:a {}}})
         {:foo #{{:a 1} {}}}))

  (testing "keep meta"
    (is (= (-> (psd/select-shape ^:yes? {:foo [{:a 1 :b 2}
                                               {:a 3 :b 4}]} {:foo {:a {}}})
               (meta))
           {:yes? true}))))

(deftest select-shape-filtering-test
  (is (= (psd/select-shape-filtering {} {})
         {}))

  (is (= (psd/select-shape-filtering {:foo "bar"} {})
         {}))

  (is (= (psd/select-shape-filtering {:foo "bar"} {:foo {}})
         {:foo "bar"}))

  (is (= (psd/select-shape-filtering {:foo "bar"} {:foo {} :baz {}})
         {:foo "bar"}))

  (is (= (psd/select-shape-filtering {:foo "bar" :baz "baz"} {:foo {}})
         {:foo "bar"}))

  (is (= (psd/select-shape-filtering {:foo {:a 1 :b 2}} {:foo {}})
         {:foo {:a 1 :b 2}}))

  (is (= (psd/select-shape-filtering {:foo {:a 1 :b 2}} {:foo {:a {}}})
         {:foo {:a 1}}))

  (is (= (psd/select-shape-filtering {:foo [{:a 1 :b 2}
                                            {:a 3 :b 4}]} {:foo {:a {}}})
         {:foo [{:a 1}
                {:a 3}]}))

  (is (= (psd/select-shape-filtering {:foo [{:a 1 :b 2}
                                            {:b 4}]} {:foo {:a {}}})
         {:foo [{:a 1}]}))

  (is (= (psd/select-shape-filtering {:foo [{:a 1 :b 2}
                                            {:b 4}]} {:foo {:a {}}} {:foo {}})
         {:foo [{:a 1} {}]}))

  (is (= (psd/select-shape-filtering {:foo #{{:a 1 :b 2}
                                             {:a 3 :b 4}}} {:foo {:a {}}})
         {:foo #{{:a 1}
                 {:a 3}}}))

  (is (= (psd/select-shape-filtering {:foo #{{:a 1 :b 2}
                                             {:b 4}}} {:foo {:a {}}})
         {:foo #{{:a 1}}}))

  (testing "keep meta"
    (is (= (-> (psd/select-shape-filtering ^:yes? {:foo [{:a 1 :b 2}
                                                         {:a 3 :b 4}]} {:foo {:a {}}})
               (meta))
           {:yes? true}))))
