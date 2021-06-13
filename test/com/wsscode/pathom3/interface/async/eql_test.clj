(ns com.wsscode.pathom3.interface.async.eql-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.interface.async.eql :as p.a.eql]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]
    [promesa.core :as p]))

(def registry
  [geo/full-registry
   (pbir/constantly-resolver :simple "value")
   (pbir/constantly-fn-resolver :foo ::foo)])

(deftest boundary-interface-test
  (let [fi (p.a.eql/boundary-interface (pci/register registry))]
    (testing "call with just tx"
      (is (= @(fi [:simple])
             {:simple "value"})))

    (testing "call with entity and tx"
      (is (= @(fi {:pathom/entity {:left 10}
                   :pathom/eql    [:x]})
             {:x 10})))

    (testing "merge env"
      (is (= @(fi [:foo])
             {:foo nil}))

      (is (= @(fi {::foo "bar"} [:foo])
             {:foo "bar"})))

    (testing "modify env"
      (is (= @(fi #(pci/register % (pbir/constantly-resolver :new "value")) [:new])
             {:new "value"}))))

  (testing "async env"
    (let [fi (p.a.eql/boundary-interface (p/promise (pci/register registry)))]
      (is (= @(fi [:simple])
             {:simple "value"}))

      (testing "providing extra async env"
        (is (= @(fi (p/promise {::foo "bar"}) [:foo])
               {:foo "bar"}))))))
