(ns com.wsscode.pathom3.connect.operation.transit-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.operation.transit :as pcot]
    [com.wsscode.transito :as transito]))

(defn read-transit-str [^String s]
  (transito/read-str s {:handlers pcot/read-handlers}))

(defn write-transit-str [o]
  (transito/write-str o {:handlers pcot/write-handlers}))

(deftest encode-decode-resolvers-test
  (is (= (-> (pco/resolver 'r {::pco/output [:a]} (fn [_ _]))
             (write-transit-str)
             (read-transit-str)
             (pco/operation-config))
         '{::pco/input    [],
           ::pco/provides {:a {}},
           ::pco/output   [:a],
           ::pco/op-name  r,
           ::pco/requires {}}))

  (is (= (-> (pco/mutation 'm {::pco/params [:a]} (fn [_ _]))
             (write-transit-str)
             (read-transit-str)
             (pco/operation-config))
         '{::pco/params  [:a],
           ::pco/op-name m})))
