(ns com.wsscode.pathom3.interface.async.eql-test
  (:require
    [check.core :refer [check]]
    [clojure.test :refer [deftest is testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.interface.async.eql :as p.a.eql]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]
    [promesa.core :as p]))

(declare =>)

(def registry
  [geo/full-registry
   (pbir/constantly-resolver :simple "value")
   (pbir/constantly-fn-resolver :foo ::foo)])

(defn run-boundary-interface [env request]
  (let [fi (p.a.eql/boundary-interface env)]
    @(fi request)))

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
               {:foo "bar"})))))

  (testing "error reporting"
    (check (=>
             {:com.wsscode.pathom3.error/error-message     "Resolver error exception at path []: Err",
              :com.wsscode.pathom3.error/error-stack       #"Resolver error exception"
              :com.wsscode.pathom3.connect.planner/graph   {:com.wsscode.pathom3.connect.planner/source-ast                {:type     :root,
                                                                                                                            :children [{:type         :prop,
                                                                                                                                        :dispatch-key :error,
                                                                                                                                        :key          :error}]},
                                                            :com.wsscode.pathom3.connect.planner/index-attrs               {:error #{1}},
                                                            :com.wsscode.pathom3.connect.runner/compute-plan-run-finish-ms number?,
                                                            :com.wsscode.pathom3.connect.runner/graph-run-finish-ms        number?,
                                                            :com.wsscode.pathom3.connect.runner/compute-plan-run-start-ms  number?,
                                                            :com.wsscode.pathom3.connect.planner/root                      1,
                                                            :com.wsscode.pathom3.connect.planner/available-data            {},
                                                            :com.wsscode.pathom3.connect.runner/node-run-stats             {1 {:com.wsscode.pathom3.connect.runner/node-run-start-ms     number?,
                                                                                                                               :com.wsscode.pathom3.connect.runner/resolver-run-start-ms number?}},
                                                            :com.wsscode.pathom3.connect.planner/index-ast                 {:error {:type         :prop,
                                                                                                                                    :dispatch-key :error,
                                                                                                                                    :key          :error}},
                                                            :com.wsscode.pathom3.connect.runner/graph-run-start-ms         number?,
                                                            :com.wsscode.pathom3.connect.planner/index-resolver->nodes     {'error #{1}},
                                                            :com.wsscode.pathom3.connect.planner/nodes                     {1 {:com.wsscode.pathom3.connect.operation/op-name 'error,
                                                                                                                               :com.wsscode.pathom3.connect.planner/expects   {:error {}},
                                                                                                                               :com.wsscode.pathom3.connect.planner/input     {},
                                                                                                                               :com.wsscode.pathom3.connect.planner/node-id   1}}},
              :com.wsscode.pathom3.entity-tree/entity-tree {},
              :com.wsscode.pathom3.path/path               []}
             (run-boundary-interface
               (pci/register
                 (pco/resolver 'error
                   {::pco/output [:error]}
                   (fn [_ _]
                     (throw (ex-info "Err" {})))))
               {:pathom/eql [:error]})))

    (testing "partial success"
      (check (=>
               {:com.wsscode.pathom3.error/error-message     "Resolver error exception at path []: Err",
                :com.wsscode.pathom3.error/error-stack       #"Resolver error exception"
                :com.wsscode.pathom3.connect.planner/graph   {:com.wsscode.pathom3.connect.planner/source-ast                {:type     :root,
                                                                                                                              :children [{:type         :prop,
                                                                                                                                          :dispatch-key :error,
                                                                                                                                          :key          :error}]},
                                                              :com.wsscode.pathom3.connect.planner/index-attrs               {:error #{1},
                                                                                                                              :input #{2}},
                                                              :com.wsscode.pathom3.connect.runner/compute-plan-run-finish-ms number?,
                                                              :com.wsscode.pathom3.connect.runner/graph-run-finish-ms        number?,
                                                              :com.wsscode.pathom3.connect.runner/compute-plan-run-start-ms  number?,
                                                              :com.wsscode.pathom3.connect.planner/root                      2,
                                                              :com.wsscode.pathom3.connect.planner/available-data            {},
                                                              :com.wsscode.pathom3.connect.runner/node-run-stats             {2 {:com.wsscode.pathom3.connect.runner/node-run-start-ms      number?,
                                                                                                                                 :com.wsscode.pathom3.connect.runner/resolver-run-start-ms  number?,
                                                                                                                                 :com.wsscode.pathom3.connect.runner/resolver-run-finish-ms number?,
                                                                                                                                 :com.wsscode.pathom3.connect.runner/node-resolver-input    {},
                                                                                                                                 :com.wsscode.pathom3.connect.runner/node-resolver-output   {:input "in"},
                                                                                                                                 :com.wsscode.pathom3.connect.runner/node-done?             true,
                                                                                                                                 :com.wsscode.pathom3.connect.runner/node-run-finish-ms     number?},
                                                                                                                              1 {:com.wsscode.pathom3.connect.runner/node-run-start-ms     number?,
                                                                                                                                 :com.wsscode.pathom3.connect.runner/resolver-run-start-ms number?}},
                                                              :com.wsscode.pathom3.connect.planner/index-ast                 {:error {:type         :prop,
                                                                                                                                      :dispatch-key :error,
                                                                                                                                      :key          :error}},
                                                              :com.wsscode.pathom3.connect.runner/graph-run-start-ms         number?,
                                                              :com.wsscode.pathom3.connect.planner/index-resolver->nodes     {'error                     #{1},
                                                                                                                              '-unqualified/input--const #{2}},
                                                              :com.wsscode.pathom3.connect.planner/nodes                     {1 {:com.wsscode.pathom3.connect.operation/op-name    'error,
                                                                                                                                 :com.wsscode.pathom3.connect.planner/expects      {:error {}},
                                                                                                                                 :com.wsscode.pathom3.connect.planner/input        {:input {}},
                                                                                                                                 :com.wsscode.pathom3.connect.planner/node-id      1,
                                                                                                                                 :com.wsscode.pathom3.connect.planner/node-parents #{2}},
                                                                                                                              2 {:com.wsscode.pathom3.connect.operation/op-name '-unqualified/input--const,
                                                                                                                                 :com.wsscode.pathom3.connect.planner/expects   {:input {}},
                                                                                                                                 :com.wsscode.pathom3.connect.planner/input     {},
                                                                                                                                 :com.wsscode.pathom3.connect.planner/node-id   2,
                                                                                                                                 :com.wsscode.pathom3.connect.planner/run-next  1}}},
                :com.wsscode.pathom3.entity-tree/entity-tree {:input "in"},
                :com.wsscode.pathom3.path/path               []}
               (run-boundary-interface
                 (pci/register
                   [(pbir/constantly-resolver :input "in")
                    (pco/resolver 'error
                      {::pco/input  [:input]
                       ::pco/output [:error]}
                      (fn [_ _]
                        (throw (ex-info "Err" {}))))])
                 {:pathom/eql    [:error]
                  :pathom/entity {}}))))

    (testing "nested error"
      (check
        (=> {:com.wsscode.pathom3.connect.planner/graph
             {:com.wsscode.pathom3.connect.planner/source-ast
              {:children [{:key :error, :type :prop, :dispatch-key :error}],
               :key :foo,
               :type :join,
               :dispatch-key :foo,
               :query [:error]},
              :com.wsscode.pathom3.connect.planner/index-attrs {:error #{1}},
              :com.wsscode.pathom3.connect.runner/compute-plan-run-finish-ms
              number?,
              :com.wsscode.pathom3.connect.runner/graph-run-finish-ms
              number?,
              :com.wsscode.pathom3.connect.runner/compute-plan-run-start-ms
              number?,
              :com.wsscode.pathom3.connect.planner/root 1,
              :com.wsscode.pathom3.connect.planner/available-data {:x {}},
              :com.wsscode.pathom3.connect.runner/node-run-stats
              {1
               {:com.wsscode.pathom3.connect.runner/node-run-start-ms
                number?,
                :com.wsscode.pathom3.connect.runner/resolver-run-start-ms
                number?}},
              :com.wsscode.pathom3.connect.planner/index-ast
              {:error {:key :error, :type :prop, :dispatch-key :error}},
              :com.wsscode.pathom3.connect.runner/graph-run-start-ms
              number?,
              :com.wsscode.pathom3.connect.planner/index-resolver->nodes
              {'error #{1}},
              :com.wsscode.pathom3.connect.planner/nodes
              {1
               {:com.wsscode.pathom3.connect.operation/op-name 'error,
                :com.wsscode.pathom3.connect.planner/expects   {:error {}},
                :com.wsscode.pathom3.connect.planner/input     {},
                :com.wsscode.pathom3.connect.planner/node-id   1}}},
             :com.wsscode.pathom3.path/path               [:foo],
             :com.wsscode.pathom3.error/error-message
             "Resolver error exception at path [:foo]: Err",
             :com.wsscode.pathom3.error/error-stack
             #"Resolver error exception"
             :com.wsscode.pathom3.entity-tree/entity-tree {:x 10}}
            (run-boundary-interface
              (pci/register
                [(pco/resolver 'error
                   {::pco/output [:error]}
                   (fn [_ _]
                     (throw (ex-info "Err" {}))))])
              {:pathom/eql    [{:foo [:error]}]
               :pathom/entity {:foo {:x 10}}}))))))
