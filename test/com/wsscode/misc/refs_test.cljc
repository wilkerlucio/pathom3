(ns com.wsscode.misc.refs-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.misc.refs :refer [atom?]]))

(deftest atom?-test
  (is (true? (atom? (atom "x"))))
  (is (false? (atom? "x"))))
