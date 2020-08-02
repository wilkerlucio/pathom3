(ns com.wsscode.pathom3.entity-tree-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.entity-tree :as pe]))

(deftest merge-entity-data-test
  (is (= (pe/merge-entity-data
           {:foo "bar"}
           {:buz "baz"})
         {:foo "bar" :buz "baz"})))
