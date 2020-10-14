(ns com.wsscode.pathom3.path-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.path :as p.path]))

(deftest append-path-test
  (is (= (p.path/append-path {::p.path/path []} :foo)
         {::p.path/path [:foo]}))

  (is (= (p.path/append-path {::p.path/path [:one]} :foo)
         {::p.path/path [:one :foo]})))
