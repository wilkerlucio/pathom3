(ns com.wsscode.pathom3.interface.async.eql-test
  (:require
    [check.core :refer [=> check]]
    [clojure.string :as string]
    [clojure.test :refer [deftest is testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.interface.async.eql :as p.a.eql]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]
    [promesa.core :as p]))

(def registry
  [geo/full-registry
   (pbir/constantly-resolver :simple "value")
   (pbir/constantly-fn-resolver :foo ::foo)
   (pbir/constantly-resolver :false false)])

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
             {:com.wsscode.pathom3.error/error-message     "Resolver error exception: Err",
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
               {:com.wsscode.pathom3.error/error-message     "Resolver error exception: Err",
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

(deftest boundary-interface-include-stats-test
  (testing "omit stats by default"
    (is (nil?
          (-> (run-boundary-interface
                (pci/register
                  [(pbir/constantly-resolver :a 10)])
                {:pathom/eql [:a]})
              meta
              :com.wsscode.pathom3.connect.runner/run-stats))))

  (testing "include when requested"
    (is (some?
          (-> (run-boundary-interface
                (pci/register
                  [(pbir/constantly-resolver :a 10)])
                {:pathom/eql            [:a]
                 :pathom/include-stats? true})
              meta
              :com.wsscode.pathom3.connect.runner/run-stats)))))

(deftest process-one-test
  (is (= @(p.a.eql/process-one (pci/register registry) {:left 10 :right 30} :width)
         20))

  (is (= @(p.a.eql/process-one (pci/register geo/full-registry)
                               {:left 10 :top 5}
                               {::geo/turn-point [:right]})
         {:right 10}))

  (testing "keeps meta"
    (let [response @(p.a.eql/process-one
                      (pci/register
                        [(pbir/constantly-resolver :items [{:a 1}])])
                      :items)]
      (is (= response [{:a 1}]))
      (check
        (meta response)
        => {:com.wsscode.pathom3.connect.runner/run-stats
            {:com.wsscode.pathom3.connect.planner/source-ast            {},
             :com.wsscode.pathom3.connect.planner/index-attrs           {},
             :com.wsscode.pathom3.connect.planner/user-request-shape    {},
             :com.wsscode.pathom3.connect.planner/root                  number?,
             :com.wsscode.pathom3.connect.planner/available-data        {},
             :com.wsscode.pathom3.connect.runner/node-run-stats         {},
             :com.wsscode.pathom3.connect.planner/index-ast             {},
             :com.wsscode.pathom3.connect.runner/transient-stats        {},
             :com.wsscode.pathom3.connect.planner/index-resolver->nodes {},
             :com.wsscode.pathom3.connect.planner/nodes                 {}}}))

    (testing "don't change data when its already there"
      (let [response @(p.a.eql/process-one
                        (pci/register
                          [(pbir/constantly-resolver :items {:a 1})
                           (pbir/alias-resolver :a :b)])
                        {:items [:b]})]
        (is (= response {:b 1}))
        (check
          (meta response)
          => {:com.wsscode.pathom3.connect.runner/run-stats
              {:com.wsscode.pathom3.connect.planner/available-data
               {:a {}}}})))

    (testing "returns false"
      (is (= @(p.a.eql/process-one (pci/register registry) :false)
             false)))))

(deftest avoid-huge-ex-message
  (let [env (pci/register (pco/resolver `a {::pco/output [:a]}
                            (fn [_ _]
                              (throw (ex-info "hello"
                                              {:world 42})))))
        ex  (try
              (deref (p.a.eql/process env
                                      [:a]))
              (catch Throwable ex
                ex))
        msg (ex-message ex)]
    (testing
      "Not a huge size"
      (is (< (count msg)
             1e3)))
    (testing
      "starts with the cause message"
      (is (string/starts-with? msg "Graph execution failed: ")))
    (testing
      "Ends with the root cause message"
      (is (string/ends-with? msg ": hello")))))
