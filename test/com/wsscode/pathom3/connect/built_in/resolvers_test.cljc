(ns com.wsscode.pathom3.connect.built-in.resolvers-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.interface.smart-map :as psm]))

(deftest alias-resolver-test
  (is (= ((pbir/alias-resolver :foo :bar) {} {:foo 3})
         {:bar 3})))

(deftest constantly-resolver-test
  (is (= ((pbir/constantly-resolver :foo "bar"))
         {:foo "bar"}))

  (testing "output inference"
    (is (= (-> (pco/operation-config (pbir/constantly-resolver :foo {:bar "baz"}))
               ::pco/output)
           [{:foo [:bar]}]))

    (is (= (-> (pco/operation-config (pbir/constantly-resolver :foo [{:bar "baz"}
                                                                     {:other "ble"}]))
               ::pco/output)
           [{:foo [:bar :other]}]))))

(deftest constantly-fn-resolver-test
  (is (= ((pbir/constantly-fn-resolver :foo (fn [_] "bar")))
         {:foo "bar"})))

(deftest single-attr-resolver-test
  (is (= ((pbir/single-attr-resolver :n :x inc) {:n 10})
         {:x 11})))

(deftest single-attr-with-env-resolver-test
  (is (= ((pbir/single-attr-with-env-resolver :n :x #(+ (:add %1) %2))
          {:add 5}
          {:n 10})
         {:x 15})))

(deftest map-table-resolver-test
  (let [resolver (pbir/static-table-resolver ::id {1 {::color "Gray"}
                                                   2 {::color "Purple"}})
        config   (pco/operation-config resolver)]
    (is (= (resolver {::id 2})
           {::color "Purple"}))
    (is (= (resolver {::id 3}) nil))
    (is (= (::pco/input config)
           [::id]))
    (is (= (::pco/output config)
           [::color]))))

(deftest attribute-map-resolver-test
  (let [resolver (pbir/static-attribute-map-resolver ::id ::color
                   {1 "Gray"
                    2 "Purple"})
        config   (pco/operation-config resolver)]
    (is (= (resolver {::id 2})
           {::color "Purple"}))
    (is (= (resolver {::id 3}) nil))
    (is (= (::pco/input config)
           [::id]))
    (is (= (::pco/output config)
           [::color]))))

(deftest attribute-table-resolver-test
  (let [resolver (pbir/attribute-table-resolver ::colors ::id [::color])
        config   (pco/operation-config resolver)]
    (is (= (resolver {::colors {1 {::color "Gray"}
                                2 {::color "Purple"}}
                      ::id     2})
           {::color "Purple"}))
    (is (= (::pco/input config)
           [::id ::colors]))
    (is (= (::pco/output config)
           [::color]))))

(deftest env-table-resolver-test
  (let [resolver (pbir/env-table-resolver ::colors ::id [::color])
        config   (pco/operation-config resolver)]
    (is (= (resolver
             {::colors {1 {::color "Gray"}
                        2 {::color "Purple"}}}
             {::id 2})
           {::color "Purple"}))
    (is (= (::pco/input config)
           [::id]))
    (is (= (::pco/output config)
           [::color]))))

(deftest edn-file-resolver-test
  (let [[resolver :as resolvers] (pbir/edn-file-resolver "test/resources/sample-config.edn")]
    (is (= (::pco/output (pco/operation-config resolver))
           [:my.system/generic-db :my.system/initial-path :my.system/port]))

    (is (= (resolver {} {})
           #:my.system{:initial-path "/tmp/system"
                       :port         1234
                       :generic-db   {4 {:my.system.user/name "Anne"}
                                      2 {:my.system.user/name "Fred"}}}))

    (let [sm (psm/smart-map (pci/register resolvers) {:my.system/user-id 4})]
      (is (= (:my.system.user/name sm) "Anne")))))

(deftest global-data-resolver-test
  (let [[resolver :as resolvers] (pbir/global-data-resolver
                                   {:my.system/port
                                    1234

                                    :my.system/initial-path
                                    "/tmp/system"

                                    :my.system/generic-db
                                    ^{:com.wsscode.pathom3/entity-table :my.system/user-id}
                                    {4 {:my.system.user/name "Anne"}
                                     2 {:my.system.user/name "Fred"}}})]
    (is (= (::pco/output (pco/operation-config resolver))
           [:my.system/generic-db :my.system/initial-path :my.system/port]))

    (is (= (resolver {} {})
           #:my.system{:initial-path "/tmp/system"
                       :port         1234
                       :generic-db   {4 {:my.system.user/name "Anne"}
                                      2 {:my.system.user/name "Fred"}}}))

    (let [sm (psm/smart-map (pci/register resolvers) {:my.system/user-id 4})]
      (is (= (:my.system.user/name sm) "Anne")))))
