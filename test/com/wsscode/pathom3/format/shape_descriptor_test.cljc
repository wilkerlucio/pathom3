(ns com.wsscode.pathom3.format.shape-descriptor-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.test.helpers :as h]))

(deftest merge-shapes-test
  (is (= (pfsd/merge-shapes
           {} {})
         {}))
  (is (= (pfsd/merge-shapes
           {:a {}} {})
         {:a {}}))

  (is (= (pfsd/merge-shapes
           {:a {}} {:b {}})
         {:a {} :b {}}))

  (is (= (-> (pfsd/merge-shapes
               {:a ^:foo {}} {:a {}})
             (h/expose-meta))
         {:a {::h/meta {:foo true}}}))

  (is (= (-> (pfsd/merge-shapes
               {:a {}} {:a ^:foo {}})
             (h/expose-meta))
         {:a {::h/meta {:foo true}}})))

(deftest query->shape-descriptor-test
  (testing "empty query"
    (is (= (pfsd/query->shape-descriptor
             [])
           {})))

  (testing "single attribute"
    (is (= (pfsd/query->shape-descriptor
             [:foo])
           {:foo {}})))

  (testing "multiple attributes and nesting"
    (is (= (pfsd/query->shape-descriptor
             [{:foo [:bar]} :baz])
           {:foo {:bar {}}
            :baz {}})))

  (testing "combining union queries"
    (is (= (pfsd/query->shape-descriptor
             [{:foo {:a [:x] :b [:y]}}])
           {:foo {:x {} :y {}}})))

  (testing "retain params"
    (is (= (-> (pfsd/query->shape-descriptor
                 [{:a [(list :b {:p "v"})]}])
               (h/expose-meta))
           {:a {:b {::h/meta {::pfsd/params {:p "v"}}}}}))))

(deftest shape-descriptor->ast-test
  (testing "empty query"
    (is (= (pfsd/shape-descriptor->ast
             {})
           {:children []
            :type     :root})))

  (testing "single attribute"
    (is (= (pfsd/shape-descriptor->ast
             {:foo {}})
           {:type :root, :children [{:type :prop, :dispatch-key :foo, :key :foo}]})))

  (testing "multiple attributes and nesting"
    (is (= (pfsd/shape-descriptor->ast
             {:foo {:bar {}}
              :baz {}})
           {:type     :root,
            :children [{:type         :join,
                        :dispatch-key :foo,
                        :key          :foo,
                        :children     [{:type :prop, :dispatch-key :bar, :key :bar}]}
                       {:type :prop, :dispatch-key :baz, :key :baz}]})))

  (testing "union"
    (is (= (pfsd/shape-descriptor->ast
             {:x ^::pfsd/union? {:a {:aa {}}
                                 :b {:bb {}}}})
           {:type     :root,
            :children [{:type         :join,
                        :dispatch-key :x,
                        :key          :x,
                        :children     [{:type     :union,
                                        :children [{:type      :union-entry,
                                                    :union-key :a,
                                                    :children  [{:type         :prop,
                                                                 :dispatch-key :aa,
                                                                 :key          :aa}]}
                                                   {:type      :union-entry,
                                                    :union-key :b,
                                                    :children  [{:type         :prop,
                                                                 :dispatch-key :bb,
                                                                 :key          :bb}]}]}]}]})))

  (testing "retain params"
    (is (= (-> (pfsd/query->shape-descriptor
                 [{:a [(list :b {:p "v"})]}])
               (pfsd/shape-descriptor->ast))
           {:type     :root,
            :children [{:type         :join,
                        :key          :a,
                        :dispatch-key :a,
                        :children     [{:type :prop, :key :b, :dispatch-key :b, :params {:p "v"}}]}]}))))

(deftest shape-descriptor->query-test
  (testing "empty query"
    (is (= (pfsd/shape-descriptor->query
             {})
           [])))

  (testing "single attribute"
    (is (= (pfsd/shape-descriptor->query
             {:foo {}})
           [:foo])))

  (testing "multiple attributes and nesting"
    (is (= (pfsd/shape-descriptor->query
             {:foo {:bar {}}
              :baz {}})
           [{:foo [:bar]} :baz])))

  (testing "union"
    (is (= (pfsd/shape-descriptor->query
             {:x ^::pfsd/union? {:a {:aa {}}
                                 :b {:bb {}}}})
           [{:x {:a [:aa] :b [:bb]}}]))

    (is (= (pfsd/shape-descriptor->query
             {:x ^::pfsd/union? {:a {:aa {}}
                                 :b {:bb {}}
                                 :c {}}})
           [{:x {:a [:aa] :b [:bb] :c []}}])))

  (testing "retain params"
    (is (= (-> (pfsd/query->shape-descriptor
                 [{:a [(list :b {:p "v"})]}])
               (pfsd/shape-descriptor->query))
           [{:a [(list :b {:p "v"})]}]))))

(deftest data->shape-descriptor-test
  (is (= (pfsd/data->shape-descriptor {})
         {}))

  (is (= (pfsd/data->shape-descriptor {:foo "bar"})
         {:foo {}}))

  (is (= (pfsd/data->shape-descriptor {:foo {:bar "baz"}})
         {:foo {:bar {}}}))

  (is (= (pfsd/data->shape-descriptor {:foo [{:bar "x"}
                                             {:baz "a"}]})
         {:foo {:bar {}
                :baz {}}})))

(deftest relax-empty-collections-test
  (is (= (pfsd/relax-empty-collections
           {:foo {:bar {}}} {:foo []})
         {:foo {}}))

  (is (= (pfsd/relax-empty-collections
           {:foo {:bar {:baz {}}}}
           {:foo {:bar []}})
         {:foo {:bar {}}}))

  (is (= (pfsd/relax-empty-collections
           {:foo {:bar {:baz {}}}}
           {:foo [{:bar []}]})
         {:foo {:bar {}}})))

(deftest missing-test
  (is (= (pfsd/missing {} {})
         nil))

  (is (= (pfsd/missing {} {:foo {}})
         {:foo {}}))

  (is (= (pfsd/missing {} {:foo {:bar {}}})
         {:foo {:bar {}}}))

  (is (= (pfsd/missing {:foo {}} {})
         nil))

  (is (= (pfsd/missing {:foo {}} {:foo {}})
         nil))

  (is (= (pfsd/missing {:foo {}} {:foo {} :bar {}})
         {:bar {}}))

  (is (= (pfsd/missing {:foo {:bar {}}} {:foo {:bar {}}})
         nil))

  (is (= (pfsd/missing {:foo {}} {:foo {:bar {}}})
         {:foo {:bar {}}}))

  (is (= (pfsd/missing {:foo {:bar {}}} {:foo {:bar {} :baz {}}})
         {:foo {:baz {}}}))

  (testing "with data"
    (is (= (pfsd/missing {:foo {}} {:foo {:bar {}}} {:foo []})
           nil))

    (is (= (pfsd/missing {:foo {:bar {}}} {:foo {:bar {:baz {}}}}
                         {:foo {:bar []}})
           nil))))

(deftest missing-from-data-test
  (is (= (pfsd/missing-from-data {} {})
         nil))

  (is (= (pfsd/missing-from-data {} {:foo {}})
         {:foo {}}))

  (is (= (pfsd/missing-from-data {} {:foo {:bar {}}})
         {:foo {:bar {}}}))

  (is (= (pfsd/missing-from-data {:foo "bar"} {})
         nil))

  (is (= (pfsd/missing-from-data {:foo "bar"} {:foo {}})
         nil))

  (is (= (pfsd/missing-from-data {:foo "bar"} {:foo {} :bar {}})
         {:bar {}}))

  (is (= (pfsd/missing-from-data {:foo {:bar "bar"}} {:foo {:bar {}}})
         nil))

  (is (= (pfsd/missing-from-data {:foo {}} {:foo {:bar {}}})
         {:foo {:bar {}}}))

  (is (= (pfsd/missing-from-data {:foo {:bar {}}} {:foo {:bar {} :baz {}}})
         {:foo {:baz {}}}))

  (testing "handling collections"
    (is (= (pfsd/missing-from-data {:foo [{:bar "bar"}]} {:foo {:bar {}}})
           nil))

    (is (= (pfsd/missing-from-data {:foo [{:bar "bar"}]} {:foo {:bar {} :baz {}}})
           {:foo {:baz {}}}))

    (is (= (pfsd/missing-from-data {:foo [{:bar "bar"}
                                          {:bar "bar" :baz "other"}]}
                                   {:foo {:bar {} :baz {}}})
           {:foo {:baz {}}}))))

(deftest difference-test
  (is (= (pfsd/difference {} {})
         {}))

  (is (= (pfsd/difference {:a {}} {})
         {:a {}}))

  (is (= (pfsd/difference {:a {}} {:b {}})
         {:a {}}))

  (is (= (pfsd/difference {:a {}} {:a {}})
         {}))

  (testing "nested"
    (is (= (pfsd/difference {:a {:b {}}} {:a {}})
           {}))

    (is (= (pfsd/difference {:a {}} {:a {:b {}}})
           {}))

    (is (= (pfsd/difference {:a {:b {}}} {:a {:b {}}})
           {}))

    (is (= (pfsd/difference {:a {:b {}}} {:a {:c {}}})
           {:a {:b {}}}))

    (is (= (pfsd/difference {:a {:b {} :c {}}} {:a {:b {}}})
           {:a {:c {}}}))))

(deftest intersection-test
  (is (= (pfsd/intersection {} {})
         {}))

  (is (= (pfsd/intersection {:a {}} {})
         {}))

  (is (= (pfsd/intersection {:a {}} {:b {}})
         {}))

  (is (= (pfsd/intersection {:a {}} {:a {}})
         {:a {}}))

  (testing "params"
    (is (= (-> (pfsd/intersection {:a ^{::pfsd/params {:p "v"}} {}} {:a {}})
               (h/expose-meta))
           {:a {::h/meta {::pfsd/params {:p "v"}}}})))

  (testing "nested"
    (is (= (pfsd/intersection {:a {:b {}}} {:a {}})
           {:a {}}))

    (is (= (pfsd/intersection {:a {}} {:a {:b {}}})
           {:a {}}))

    (is (= (pfsd/intersection {:a {:b {}}} {:a {:b {}}})
           {:a {:b {}}}))

    (is (= (pfsd/intersection {:a {:b {}}} {:a {:c {}}})
           {:a {}}))

    (is (= (pfsd/intersection {:a {:b {} :c {}}} {:a {:b {}}})
           {:a {:b {}}}))))

(deftest select-shape-test
  (is (= (pfsd/select-shape {} {})
         {}))

  (is (= (pfsd/select-shape {:foo "bar"} {})
         {}))

  (is (= (pfsd/select-shape {:foo "bar"} {:foo {}})
         {:foo "bar"}))

  (is (= (pfsd/select-shape {:foo "bar"} {:foo {} :baz {}})
         {:foo "bar"}))

  (is (= (pfsd/select-shape {:foo "bar" :baz "baz"} {:foo {}})
         {:foo "bar"}))

  (is (= (pfsd/select-shape {:foo {:a 1 :b 2}} {:foo {}})
         {:foo {:a 1 :b 2}}))

  (is (= (pfsd/select-shape {:foo {:a 1 :b 2}} {:foo {:a {}}})
         {:foo {:a 1}}))

  (is (= (pfsd/select-shape {:foo [{:a 1 :b 2}
                                   {:a 3 :b 4}]} {:foo {:a {}}})
         {:foo [{:a 1}
                {:a 3}]}))

  (is (= (pfsd/select-shape {:foo [{:a 1 :b 2}
                                   {:b 4}]} {:foo {:a {}}})
         {:foo [{:a 1}
                {}]}))

  (is (= (pfsd/select-shape {:foo #{{:a 1 :b 2}
                                    {:a 3 :b 4}}} {:foo {:a {}}})
         {:foo #{{:a 1}
                 {:a 3}}}))

  (is (= (pfsd/select-shape {:foo #{{:a 1 :b 2}
                                    {:b 4}}} {:foo {:a {}}})
         {:foo #{{:a 1} {}}}))

  (testing "keep meta"
    (is (= (-> (pfsd/select-shape ^:yes? {:foo [{:a 1 :b 2}
                                                {:a 3 :b 4}]} {:foo {:a {}}})
               (meta))
           {:yes? true}))))

(deftest select-shape-filtering-test
  (is (= (pfsd/select-shape-filtering {} {})
         {}))

  (is (= (pfsd/select-shape-filtering {:foo "bar"} {})
         {}))

  (is (= (pfsd/select-shape-filtering {:foo "bar"} {:foo {}})
         {:foo "bar"}))

  (is (= (pfsd/select-shape-filtering {:foo "bar"} {:foo {} :baz {}})
         {:foo "bar"}))

  (is (= (pfsd/select-shape-filtering {:foo "bar" :baz "baz"} {:foo {}})
         {:foo "bar"}))

  (is (= (pfsd/select-shape-filtering {:foo {:a 1 :b 2}} {:foo {}})
         {:foo {:a 1 :b 2}}))

  (is (= (pfsd/select-shape-filtering {:foo {:a 1 :b 2}} {:foo {:a {}}})
         {:foo {:a 1}}))

  (is (= (pfsd/select-shape-filtering {:foo [{:a 1 :b 2}
                                             {:a 3 :b 4}]} {:foo {:a {}}})
         {:foo [{:a 1}
                {:a 3}]}))

  (is (= (pfsd/select-shape-filtering {:foo [{:a 1 :b 2}
                                             {:b 4}]} {:foo {:a {}}})
         {:foo [{:a 1}]}))

  (is (= (pfsd/select-shape-filtering {:foo [{:a 1 :b 2}
                                             {:b 4}]} {:foo {:a {}}} {:foo {}})
         {:foo [{:a 1} {}]}))

  (is (= (pfsd/select-shape-filtering {:foo #{{:a 1 :b 2}
                                              {:a 3 :b 4}}} {:foo {:a {}}})
         {:foo #{{:a 1}
                 {:a 3}}}))

  (is (= (pfsd/select-shape-filtering {:foo #{{:a 1 :b 2}
                                              {:b 4}}} {:foo {:a {}}})
         {:foo #{{:a 1}}}))

  (testing "keep meta"
    (is (= (-> (pfsd/select-shape-filtering ^:yes? {:foo [{:a 1 :b 2}
                                                          {:a 3 :b 4}]} {:foo {:a {}}})
               (meta))
           {:yes? true}))))

(deftest data->shape-descriptor-shallow-test
  (is (= (pfsd/data->shape-descriptor-shallow {:a 1})
         {:a {}}))

  (is (= (pfsd/data->shape-descriptor-shallow {:a {:b {:c {}}}})
         {:a {}})))
