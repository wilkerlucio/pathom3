(ns com.wsscode.pathom3.placeholder-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.path :as p.path]
    [com.wsscode.pathom3.placeholder :as p.ph]))

(deftest find-closest-non-placeholder-parent-join-key-test
  (is (= (p.ph/find-closest-non-placeholder-parent-join-key {})
         nil))

  (is (= (p.ph/find-closest-non-placeholder-parent-join-key
           {::p.path/path []})
         nil))

  (is (= (p.ph/find-closest-non-placeholder-parent-join-key
           {::p.ph/placeholder-prefixes #{">"}
            ::p.path/path               [:>/placeholder]})
         nil))

  (is (= (p.ph/find-closest-non-placeholder-parent-join-key
           {::p.ph/placeholder-prefixes #{">"}
            ::p.path/path               [:parent :>/placeholder]})
         :parent))

  (is (= (p.ph/find-closest-non-placeholder-parent-join-key
           {::p.ph/placeholder-prefixes #{">"}
            ::p.path/path               [:deeper :parent :>/placeholder]})
         :parent))

  (is (= (p.ph/find-closest-non-placeholder-parent-join-key
           {::p.ph/placeholder-prefixes #{">"}
            ::p.path/path               [:deeper [:ident "thing"] :>/placeholder :>/other-placeholder]})
         [:ident "thing"])))
