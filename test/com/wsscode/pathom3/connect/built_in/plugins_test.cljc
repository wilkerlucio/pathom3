(ns com.wsscode.pathom3.connect.built-in.plugins-test
  (:require
    [check.core :refer [check =>]]
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.log :as l]
    [com.wsscode.pathom3.connect.built-in.plugins :as pbip]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [com.wsscode.pathom3.plugin :as p.plugin]
    [spy.core :as spy]))

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

(deftest filtered-sequence-items-plugin-test
  (is (= {:items [{:x "b", :y "y"} {:x "c", :y "y2"}]}
         (-> (p.eql/process
               (-> (pci/register
                     [(pbir/global-data-resolver {:items [{:x "a"}
                                                          {:x "b"
                                                           :y "y"}
                                                          {:y "xx"}
                                                          {:x "c"
                                                           :y "y2"}]})])
                   (p.plugin/register (pbip/filtered-sequence-items-plugin)))
               [^::pbip/remove-error-items {:items [:x :y]}]))))

  (is (thrown?
        #?(:clj Throwable :cljs :default)
        (-> (p.eql/process
              (-> (pci/register
                    [(pbir/global-data-resolver {:items [{:x "a"}
                                                         {:x "b"
                                                          :y "y"}
                                                         {:y "xx"}
                                                         {:x "c"
                                                          :y "y2"}]})])
                  (p.plugin/register (pbip/filtered-sequence-items-plugin)))
              [{:items [:x :y]}]))))

  (is (= {:items [{:x "b", :y "y"} {:x "c", :y "y2"}]}
         (-> (p.eql/process
               (-> (pci/register
                     [(pbir/global-data-resolver {:items [{:x "a"}
                                                          {:x "b"
                                                           :y "y"}
                                                          {:y "xx"}
                                                          {:x "c"
                                                           :y "y2"}]})])
                   (p.plugin/register (pbip/filtered-sequence-items-plugin {::pbip/apply-everywhere? true})))
               [{:items [:x :y]}])))))

(deftest env-wrap-plugin-test
  (is (= (-> (pci/register
               (pco/resolver 'env-data
                 {::pco/output [:env-value]}
                 (fn [{:keys [data]} _]
                   {:env-value data})))
             (p.plugin/register (pbip/env-wrap-plugin #(assoc % :data "bar")))
             (p.eql/process [:env-value]))
         {:env-value "bar"})))

(deftest dev-linter-test
  (testing "simple extra key"
    (binding [l/*active-logger* (spy/spy)]
      (p.eql/process
        (-> (pci/register
              [(pco/resolver 'x
                 {::pco/output [:x]}
                 (fn [_ _] {:x 10 :y 20}))])
            (p.plugin/register (pbip/dev-linter)))
        [:x])
      (check
        (=> [[{:com.wsscode.pathom3.connect.operation/provides {:x {}},
               :com.wsscode.log/event                          :com.wsscode.pathom3.connect.built-in.plugins/undeclared-output,
               :com.wsscode.log/timestamp                      inst?,
               :com.wsscode.pathom3.connect.operation/op-name  'x,
               :com.wsscode.log/level                          :com.wsscode.log/level-warn,
               ::pbip/unexpected-shape                         {:y {}}}]]
            (spy/calls l/*active-logger*)))))

  (testing "on nested inputs"
    (binding [l/*active-logger* (spy/spy)]
      (p.eql/process
        (-> (pci/register
              [(pco/resolver 'x
                 {::pco/output [{:x [:y]}]}
                 (fn [_ _] {:x {:y 20 :z 30}}))])
            (p.plugin/register (pbip/dev-linter)))
        [:x])
      (check
        (=> [[{:com.wsscode.pathom3.connect.operation/provides {:x {}},
               :com.wsscode.log/event                          :com.wsscode.pathom3.connect.built-in.plugins/undeclared-output,
               :com.wsscode.log/timestamp                      inst?,
               :com.wsscode.pathom3.connect.operation/op-name  'x,
               :com.wsscode.log/level                          :com.wsscode.log/level-warn,
               ::pbip/unexpected-shape                         {:x {:z {}}}}]]
            (spy/calls l/*active-logger*))))))

(deftest placeholder-data-test)
