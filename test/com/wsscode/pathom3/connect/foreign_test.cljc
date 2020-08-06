(ns com.wsscode.pathom3.connect.foreign-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.foreign :as p.c.f]
    [com.wsscode.pathom3.connect.planner :as p.c.p]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.specs :as p.spec]
    [edn-query-language.core :as eql]))

(deftest remove-internal-keys-test
  (is (= (p.c.f/remove-internal-keys {:foo                   "bar"
                                      :com.wsscode/me        "value"
                                      :com.wsscode.pathom/me "value"})
         {:foo            "bar"
          :com.wsscode/me "value"})))

(deftest compute-foreign-query-test
  (testing "no inputs"
    (is (= (p.c.f/compute-foreign-query {::p.c.p/node        {::p.c.p/foreign-ast (eql/query->ast [:a])}
                                         ::p.ent/cache-tree* (atom {})})
           {::p.c.f/base-query [:a]
            ::p.c.f/query      [:a]})))

  (testing "inputs, but no parent ident, single attribute always goes as ident"
    (is (= (p.c.f/compute-foreign-query {::p.c.p/node        {::p.c.p/foreign-ast (eql/query->ast [:a])
                                                              ::p.c.p/input       {:z {}}}
                                         ::p.ent/cache-tree* (atom {:z "bar"})})
           {::p.c.f/base-query [:a]
            ::p.c.f/query      '[{([:z "bar"] {:pathom/context {}}) [:a]}]
            ::p.c.f/join-node  [:z "bar"]})))

  (testing "inputs, with parent ident"
    (is (= (p.c.f/compute-foreign-query {::p.c.p/node        {::p.c.p/foreign-ast (eql/query->ast [:a])
                                                              ::p.c.p/input       {:z {}}}
                                         ::p.spec/path       [[:z "bar"] :a]
                                         ::p.ent/cache-tree* (atom {[:z "bar"] {:a {:z "bar"}}})})
           {::p.c.f/base-query [:a]
            ::p.c.f/query      '[{([:z "bar"] {:pathom/context {}}) [:a]}]
            ::p.c.f/join-node  [:z "bar"]})))

  (testing "inputs, with parent ident"
    (is (= (p.c.f/compute-foreign-query {::p.c.p/node        {::p.c.p/foreign-ast (eql/query->ast [:a])
                                                              ::p.c.p/input       {:z {}}}
                                         ::p.spec/path       [[:z "bar"] :a]
                                         ::p.ent/cache-tree* (atom {[:z "bar"] {:a {:z "bar"}}})})
           {::p.c.f/base-query [:a]
            ::p.c.f/query      '[{([:z "bar"] {:pathom/context {}}) [:a]}]
            ::p.c.f/join-node  [:z "bar"]}))

    (testing "with multiple inputs"
      (is (= (p.c.f/compute-foreign-query {::p.c.p/node        {::p.c.p/foreign-ast (eql/query->ast [:a])
                                                                ::p.c.p/input       {:x {}
                                                                                     :z {}}}
                                           ::p.spec/path       [[:z "bar"] :a]
                                           ::p.ent/cache-tree* (atom {[:z "bar"] {:a {:x "foo"
                                                                                      :z "bar"}}})})
             {::p.c.f/base-query [:a]
              ::p.c.f/query      '[{([:z "bar"] {:pathom/context {:x "foo"}}) [:a]}]
              ::p.c.f/join-node  [:z "bar"]})))))

(deftest internalize-foreign-errors-test
  (is (= (p.c.f/internalize-foreign-errors {::p.spec/path [:a]}
                                           {[:a] "error"})
         {[:a] "error"}))

  (is (= (p.c.f/internalize-foreign-errors {::p.spec/path [:x :y :a]}
                                           {[:a]    "error"
                                            [:b :c] "error 2"})
         {[:x :y :a]    "error"
          [:x :y :b :c] "error 2"}))

  (is (= (p.c.f/internalize-foreign-errors {::p.spec/path     [:x :y :a]
                                            ::p.c.f/join-node [:z "foo"]}
                                           {[[:z "foo"] :a]    "error"
                                            [[:z "foo"] :b :c] "error 2"})
         {[:x :y :a]    "error"
          [:x :y :b :c] "error 2"})))
