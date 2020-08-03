(ns com.wsscode.pathom3.format.eql-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.format.eql :as feql]
    [edn-query-language.core :as eql]))

(deftest query-root-properties-test
  (is (= (feql/query-root-properties [{:a [:b]} :c])
         [:a :c]))

  (is (= (feql/query-root-properties {:foo [{:a [:b]} :c]
                                      :bar [:a :d]})
         [:a :c :d])))

(deftest union-children?-test
  (is (true? (feql/union-children? (eql/query->ast1 [{:union {:a [:foo]}}]))))
  (is (false? (feql/union-children? (eql/query->ast1 [:standard])))))

(deftest maybe-merge-union-ast-test
  (is (= (-> [{:union {:a [:foo]
                       :b [:bar]}}]
             (eql/query->ast1)
             (feql/maybe-merge-union-ast)
             (eql/ast->query))
         [{:union [:foo :bar]}]))

  (is (= (-> [{:not-union [:baz]}]
             (eql/query->ast1)
             (feql/maybe-merge-union-ast)
             (eql/ast->query))
         [{:not-union [:baz]}])))
