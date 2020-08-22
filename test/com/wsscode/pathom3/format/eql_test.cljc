(ns com.wsscode.pathom3.format.eql-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [edn-query-language.core :as eql]))

(deftest query-root-properties-test
  (is (= (pf.eql/query-root-properties [{:a [:b]} :c])
         [:a :c]))

  (is (= (pf.eql/query-root-properties {:foo  [{:a [:b]} :c]
                                        :bar [:a :d]})
         [:a :c :d])))

(deftest union-children?-test
  (is (true? (pf.eql/union-children? (eql/query->ast1 [{:union {:a [:foo]}}]))))
  (is (false? (pf.eql/union-children? (eql/query->ast1 [:standard])))))

(deftest maybe-merge-union-ast-test
  (is (= (-> [{:union {:a [:foo]
                       :b [:bar]}}]
             (eql/query->ast1)
             (pf.eql/maybe-merge-union-ast)
             (eql/ast->query))
         [{:union [:foo :bar]}]))

  (is (= (-> [{:not-union [:baz]}]
             (eql/query->ast1)
             (pf.eql/maybe-merge-union-ast)
             (eql/ast->query))
         [{:not-union [:baz]}])))

(deftest ident-key-test
  (is (= (pf.eql/ident-key [:foo "bar"])
         :foo)))

(deftest index-ast-test
  (is (= (pf.eql/index-ast (eql/query->ast [:foo {:bar [:baz]}]))
         {:foo {:type :prop, :dispatch-key :foo, :key :foo},
          :bar {:type :join,
                :dispatch-key :bar,
                :key :bar,
                :query [:baz],
                :children [{:type :prop, :dispatch-key :baz, :key :baz}]}})))

(deftest map-select-test
  (is (= (pf.eql/map-select {} [:foo :bar])
         {}))

  (is (= (pf.eql/map-select {:foo 123} [:foo :bar])
         {:foo 123}))

  (is (= (pf.eql/map-select {:foo {:a 1 :b 2}} [{:foo [:b]}])
         {:foo {:b 2}}))

  (is (= (pf.eql/map-select {:foo [{:a 1 :b 2}
                                   {:c 1 :b 1}
                                   {:a 1 :c 1}
                                   3]}
                            [{:foo [:b]}])
         {:foo [{:b 2} {:b 1} {} 3]}))

  (is (= (pf.eql/map-select {:foo #{{:a 1 :b 2}
                                    {:c 1 :b 1}}}
                            [{:foo [:b]}])
         {:foo #{{:b 2} {:b 1}}})))

(deftest data->shape-test
  (is (= (pf.eql/data->shape {}) []))
  (is (= (pf.eql/data->shape {:foo "bar"}) [:foo]))
  (is (= (pf.eql/data->shape {:foo {:buz "bar"}}) [{:foo [:buz]}]))
  (is (= (pf.eql/data->shape {:foo [{:buz "bar"}]}) [{:foo [:buz]}]))
  (is (= (pf.eql/data->shape {:other "key" [:complex "key"] "value"}) [:other [:complex "key"]]))
  (is (= (pf.eql/data->shape {:foo ["abc"]}) [:foo]))
  (is (= (pf.eql/data->shape {:foo [{:buz "baz"} {:it "nih"}]}) [{:foo [:buz :it]}]))
  (is (= (pf.eql/data->shape {:foo [{:buz "baz"} "abc" {:it "nih"}]}) [{:foo [:buz :it]}]))
  (is (= (pf.eql/data->shape {:z 10 :a 1 :b {:d 3 :e 4}}) [:a {:b [:d :e]} :z])))
