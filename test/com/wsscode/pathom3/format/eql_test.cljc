(ns com.wsscode.pathom3.format.eql-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.format.eql :as p.f.eql]
    [edn-query-language.core :as eql]))

(deftest query-root-properties-test
  (is (= (p.f.eql/query-root-properties [{:a [:b]} :c])
         [:a :c]))

  (is (= (p.f.eql/query-root-properties {:foo [{:a [:b]} :c]
                                         :bar    [:a :d]})
         [:a :c :d])))

(deftest union-children?-test
  (is (true? (p.f.eql/union-children? (eql/query->ast1 [{:union {:a [:foo]}}]))))
  (is (false? (p.f.eql/union-children? (eql/query->ast1 [:standard])))))

(deftest maybe-merge-union-ast-test
  (is (= (-> [{:union {:a [:foo]
                       :b [:bar]}}]
             (eql/query->ast1)
             (p.f.eql/maybe-merge-union-ast)
             (eql/ast->query))
         [{:union [:foo :bar]}]))

  (is (= (-> [{:not-union [:baz]}]
             (eql/query->ast1)
             (p.f.eql/maybe-merge-union-ast)
             (eql/ast->query))
         [{:not-union [:baz]}])))

(deftest ident-key-test
  (is (= (p.f.eql/ident-key [:foo "bar"])
         :foo)))

(deftest index-ast-test
  (is (= (p.f.eql/index-ast (eql/query->ast [:foo {:bar [:baz]}]))
         {:foo {:type :prop, :dispatch-key :foo, :key :foo},
          :bar {:type :join,
                :dispatch-key :bar,
                :key :bar,
                :query [:baz],
                :children [{:type :prop, :dispatch-key :baz, :key :baz}]}})))
