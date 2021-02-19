(ns com.wsscode.pathom3.connect.built-in.plugins-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.plugins :as pbip]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [com.wsscode.pathom3.plugin :as p.plugin]))

(deftest attribute-errors-plugin-test
  (let [err (ex-info "Err" {})]
    (is (= (p.eql/process
             (-> (pci/register (pbir/constantly-fn-resolver :error (fn [_] (throw err))))
                 (p.plugin/register (pbip/attribute-errors-plugin)))
             [:error])
           {::pcr/attribute-errors
            {:error err}})))

  (testing "only requested attributes show in errors"
    (let [err (ex-info "Err" {})]
      (is (= (p.eql/process
               (-> (pci/register [(pbir/constantly-resolver :dep 1)
                                  (pbir/single-attr-resolver :dep :error (fn [_] (throw err)))])
                   (p.plugin/register (pbip/attribute-errors-plugin)))
               [:error])
             {::pcr/attribute-errors
              {:error err}})))

    (let [err (ex-info "Err" {})]
      (is (= (p.eql/process
               (-> (pci/register [(pbir/constantly-fn-resolver :error (fn [_] (throw err)))
                                  (pbir/single-attr-resolver :error :dep inc)])
                   (p.plugin/register (pbip/attribute-errors-plugin)))
               [:dep])
             {::pcr/attribute-errors
              {:dep err}}))))

  (testing "nested"
    (let [err (ex-info "Err" {})]
      (is (= (p.eql/process
               (-> (pci/register (pbir/constantly-fn-resolver :error (fn [_] (throw err))))
                   (p.plugin/register (pbip/attribute-errors-plugin)))
               {:foo {}}
               [{:foo [:error]}])
             {:foo {::pcr/attribute-errors
                    {:error err}}}))))

  (testing "don't try to fetch errors when there are no errors"
    (is (= (p.eql/process
             (-> (pci/register (pbir/constantly-resolver :foo "bar"))
                 (p.plugin/register (pbip/attribute-errors-plugin)))
             [:foo])
           {:foo "bar"}))))

(deftest remove-stats-plugin-test
  (let [res (p.eql/process
              (-> (pci/register (pbir/constantly-resolver :foo "bar"))
                  (p.plugin/register pbip/remove-stats-plugin))
              [:foo])]
    (is (= res
           {:foo "bar"}))
    (is (= (meta res)
           {}))))

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
