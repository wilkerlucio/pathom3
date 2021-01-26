(ns com.wsscode.pathom3.connect.runner.stats-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.connect.runner.stats :as pcrs]
    [com.wsscode.pathom3.interface.smart-map :as psm]))

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

(def err (ex-info "Error" {}))

(defn error-resolver [attr]
  (pbir/constantly-fn-resolver attr (fn [_] (throw err))))

(deftest find-error-for-attribute-test
  (is (= (let [stats (-> (psm/smart-map
                           (pci/register (error-resolver :a)))
                         (psm/sm-get-with-stats :a))]
           (-> stats psm/smart-run-stats
               (pcrs/get-attribute-error :a)))
         {::pcrs/node-error-type ::pcrs/node-error-type-direct
          ::pcr/node-error       err}))

  (is (= (let [stats (-> (psm/smart-map
                           (pci/register [(error-resolver :a)
                                          (pbir/single-attr-resolver :a :b inc)]))
                         (psm/sm-get-with-stats :b))]
           (-> stats psm/smart-run-stats
               (pcrs/get-attribute-error :b)))
         {::pcrs/node-error-type ::pcrs/node-error-type-ancestor
          ::pcrs/node-error-id   2
          ::pcr/node-error       err})))
