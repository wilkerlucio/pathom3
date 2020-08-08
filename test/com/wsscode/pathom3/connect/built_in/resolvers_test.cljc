(ns com.wsscode.pathom3.connect.built-in.resolvers-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]))

(deftest alias-resolver-test
  (is (= ((pbir/alias-resolver :foo :bar) {} {:foo 3})
         {:bar 3})))
