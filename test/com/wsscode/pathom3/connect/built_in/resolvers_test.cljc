(ns com.wsscode.pathom3.connect.built-in.resolvers-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.operation :as pco]))

(deftest alias-resolver-test
  (is (= ((pbir/alias-resolver :foo :bar) {} {:foo 3})
         {:bar 3})))

(deftest edn-file-resolver-test
  (let [resolver (pbir/edn-file-resolver "resources/sample-config.edn")]
    (is (= (::pco/output (pco/operation-config resolver))
           [:my.system/initial-path :my.system/port]))
    (is (= (resolver {} {})
           #:my.system{:initial-path "/tmp/system"
                       :port         1234}))))
