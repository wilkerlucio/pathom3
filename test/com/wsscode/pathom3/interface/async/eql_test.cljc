(ns com.wsscode.pathom3.interface.async.eql-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.interface.async.eql :as p.a.eql]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]))

(def registry
  [geo/full-registry
   (pbir/constantly-resolver :simple "value")
   (pbir/constantly-fn-resolver :foo ::foo)])

#?(:clj
   (deftest foreign-interface-test
     (let [fi (p.a.eql/foreign-interface (pci/register registry))]
       (testing "call with just tx"
         (is (= @(fi [:simple])
                {:simple "value"})))

       (testing "call with entity and tx"
         (is (= @(fi {:pathom/entity {:left 10}
                      :pathom/tx     [:x]})
                {:x 10})))

       (testing "merge env"
         (is (= @(fi [:foo])
                {:foo nil}))

         (is (= @(fi {::foo "bar"} [:foo])
                {:foo "bar"})))

       (testing "modify env"
         (is (= @(fi #(pci/register % (pbir/constantly-resolver :new "value")) [:new])
                {:new "value"}))))))
