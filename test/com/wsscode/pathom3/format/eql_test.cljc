(ns com.wsscode.pathom3.format.eql-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.format.eql :as feql]))

(deftest query-root-properties-test
  (is (= (feql/query-root-properties [{:a [:b]} :c])
         [:a :c]))

  (is (= (feql/query-root-properties {:foo [{:a [:b]} :c]
                                      :bar [:a :d]})
         [:a :c :d])))
