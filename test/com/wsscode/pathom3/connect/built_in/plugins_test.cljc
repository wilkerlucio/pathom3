(ns com.wsscode.pathom3.connect.built-in.plugins-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.plugins :as pbip]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [com.wsscode.pathom3.plugin :as p.plugin]))

(deftest mutation-resolve-params-test
  (is (= (p.eql/process
           (-> (pci/register
                 [(pbir/single-attr-resolver :a :b inc)
                  (pco/mutation 'foo
                    {::pco/params [:b]}
                    (fn [_ {:keys [b]}] {:res b}))])
               (p.plugin/register pbip/mutation-resolve-params))
           ['(foo {:a 1})])
         {'foo {:res 2}})))
