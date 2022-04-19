(ns com.wsscode.pathom3.connect.runner.stats-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.connect.runner.stats :as pcrs]
    [matcher-combinators.test]))

(deftest resolver-accumulated-duration-test
  (is (= (pcrs/resolver-accumulated-duration
           {::pcr/node-run-stats
            {1 {::pcr/resolver-run-start-ms  0
                ::pcr/resolver-run-finish-ms 1}
             2 {::pcr/resolver-run-start-ms  0
                ::pcr/resolver-run-finish-ms 10}
             3 {::pcr/resolver-run-start-ms  0
                ::pcr/resolver-run-finish-ms 100}}}
           {})
         {::pcrs/resolver-accumulated-duration-ms 111})))

(deftest overhead-duration-test
  (is (= (pcrs/overhead-duration {::pcr/graph-run-duration-ms             100
                                  ::pcrs/resolver-accumulated-duration-ms 90})
         {::pcrs/overhead-duration-ms 10})))

(deftest overhead-pct-test
  (is (= (pcrs/overhead-pct {::pcr/graph-run-duration-ms 100
                             ::pcrs/overhead-duration-ms 20})
         {::pcrs/overhead-duration-percentage 0.2})))
