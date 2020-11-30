(ns com.wsscode.pathom3.connect.foreign-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.foreign :as pcf]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.path :as p.path]
    [edn-query-language.core :as eql]))

(deftest remove-internal-keys-test
  (is (= (pcf/remove-internal-keys {:foo                   "bar"
                                    :com.wsscode/me        "value"
                                    :com.wsscode.pathom/me "value"})
         {:foo            "bar"
          :com.wsscode/me "value"})))

(deftest compute-foreign-query-test
  (testing "no inputs"
    (is (= (pcf/compute-foreign-query
             (-> {::pcp/node {::pcp/foreign-ast (eql/query->ast [:a])}}
                 (p.ent/with-entity {})))
           {::pcf/base-query [:a]
            ::pcf/query      [:a]})))

  (testing "inputs, but no parent ident, single attribute always goes as ident"
    (is (= (pcf/compute-foreign-query
             (-> {::pcp/node {::pcp/foreign-ast (eql/query->ast [:a])
                              ::pcp/input       {:z {}}}}
                 (p.ent/with-entity {:z "bar"})))
           {::pcf/base-query [:a]
            ::pcf/query      '[{([:z "bar"] {:pathom/context {}}) [:a]}]
            ::pcf/join-node  [:z "bar"]})))

  (testing "inputs, with parent ident"
    (is (= (pcf/compute-foreign-query
             (-> {::pcp/node {::pcp/foreign-ast (eql/query->ast [:a])
                              ::pcp/input       {:z {}}}}
                 (p.ent/with-entity {:z "bar"})))
           {::pcf/base-query [:a]
            ::pcf/query      '[{([:z "bar"] {:pathom/context {}}) [:a]}]
            ::pcf/join-node  [:z "bar"]})))

  (testing "inputs, with parent ident"
    (is (= (pcf/compute-foreign-query
             (-> {::pcp/node {::pcp/foreign-ast (eql/query->ast [:a])
                              ::pcp/input       {:z {}}}}
                 (p.ent/with-entity {:z "bar"})))
           {::pcf/base-query [:a]
            ::pcf/query      '[{([:z "bar"] {:pathom/context {}}) [:a]}]
            ::pcf/join-node  [:z "bar"]}))

    (testing "with multiple inputs"
      (is (= (pcf/compute-foreign-query
               (-> {::pcp/node    {::pcp/foreign-ast (eql/query->ast [:a])
                                   ::pcp/input       {:x {}
                                                      :z {}}}
                    ::p.path/path [[:z "bar"] :a]}
                   (p.ent/with-entity {:x "foo"
                                       :z "bar"})))
             {::pcf/base-query [:a]
              ::pcf/query      '[{([:z "bar"] {:pathom/context {:x "foo"}}) [:a]}]
              ::pcf/join-node  [:z "bar"]})))))

(deftest internalize-foreign-errors-test
  (is (= (pcf/internalize-foreign-errors {::p.path/path [:a]}
                                         {[:a] "error"})
         {[:a] "error"}))

  (is (= (pcf/internalize-foreign-errors {::p.path/path [:x :y :a]}
                                         {[:a]    "error"
                                          [:b :c] "error 2"})
         {[:x :y :a]    "error"
          [:x :y :b :c] "error 2"}))

  (is (= (pcf/internalize-foreign-errors {::p.path/path   [:x :y :a]
                                          ::pcf/join-node [:z "foo"]}
                                         {[[:z "foo"] :a]    "error"
                                          [[:z "foo"] :b :c] "error 2"})
         {[:x :y :a]    "error"
          [:x :y :b :c] "error 2"})))
