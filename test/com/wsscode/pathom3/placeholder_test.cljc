(ns com.wsscode.pathom3.placeholder-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.placeholder :as p.ph]
    [com.wsscode.pathom3.specs :as p.spec]))

(deftest find-closest-non-placeholder-parent-join-key-test
  (is (= (p.ph/find-closest-non-placeholder-parent-join-key {})
         nil))

  (is (= (p.ph/find-closest-non-placeholder-parent-join-key
           {::p.spec/path []})
         nil))

  (is (= (p.ph/find-closest-non-placeholder-parent-join-key
           {::p.ph/placeholder-prefixes #{">"}
            ::p.spec/path               [:>/placeholder]})
         nil))

  (is (= (p.ph/find-closest-non-placeholder-parent-join-key
           {::p.ph/placeholder-prefixes #{">"}
            ::p.spec/path               [:parent :>/placeholder]})
         :parent))

  (is (= (p.ph/find-closest-non-placeholder-parent-join-key
           {::p.ph/placeholder-prefixes #{">"}
            ::p.spec/path               [:deeper :parent :>/placeholder]})
         :parent))

  (is (= (p.ph/find-closest-non-placeholder-parent-join-key
           {::p.ph/placeholder-prefixes #{">"}
            ::p.spec/path               [:deeper [:ident "thing"] :>/placeholder :>/other-placeholder]})
         [:ident "thing"])))
