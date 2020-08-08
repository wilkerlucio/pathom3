(ns com.wsscode.pathom3.connect.indexes-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]))

(deftest merge-oir-test
  (is (= (pci/merge-oir
           '{:a {#{} #{r}}}
           '{:a {#{} #{r2}}})
         '{:a {#{} #{r r2}}})))

(deftest resolver-config-test
  (let [resolver (pco/resolver 'r {::pco/output [:foo]}
                               (fn [_ _] {:foo 42}))
        env      (pci/register {}
                               resolver)]
    (is (= (pci/resolver-config env 'r)
           '{::pco/input    []
             ::pco/op-name  r
             ::pco/output   [:foo]
             ::pco/provides {:foo {}}})))

  (is (= (pci/resolver-config {} 'r) nil)))

(deftest register-test
  (let [resolver (pco/resolver 'r {::pco/output [:foo]}
                               (fn [_ _] {:foo 42}))]
    (is (= (pci/register {}
                         resolver)
           {::pci/index-resolvers {'r resolver}
            ::pci/index-oir       {:foo {#{} #{'r}}}})))

  (let [r1 (pco/resolver 'r {::pco/output [:foo]}
                         (fn [_ _] {:foo 42}))
        r2 (pco/resolver 'r2 {::pco/output [:foo2]}
                         (fn [_ _] {:foo2 "val"}))]
    (is (= (pci/register {}
                         [r1 r2])
           {::pci/index-resolvers {'r  r1
                                   'r2 r2}
            ::pci/index-oir       {:foo  {#{} #{'r}}
                                   :foo2 {#{} #{'r2}}}}))))
