(ns com.wsscode.promesa.macros-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [com.wsscode.promesa.macros :refer [clet ctry]]
    [promesa.core :as p]))

(deftest ctry-test
  (is (= :error
         (ctry
           (throw (ex-info "err" {}))
           (catch Throwable _ :error))))
  (is (= :error
         @(ctry
            (p/rejected (ex-info "err" {}))
            (catch Throwable _ :error))))
  (is (= :error
         (ctry
           (clet [foo (throw (ex-info "err" {}))]
             foo)
           (catch Throwable _ :error))))
  (is (= :error
         @(ctry
            (clet [foo (p/rejected (ex-info "err" {}))]
              foo)
            (catch Throwable _ :error)))))

