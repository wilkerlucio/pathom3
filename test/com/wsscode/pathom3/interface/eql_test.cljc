(ns com.wsscode.pathom3.interface.eql-test
  (:require
    [check.core :refer [=> check]]
    [clojure.test :refer [deftest is testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]
    [edn-query-language.core :as eql]))

(pco/defresolver coords []
  {::coords
   [{:x 10 :y 20}
    {::geo/left 20 ::geo/width 5}]})

(def registry
  [geo/full-registry
   coords
   (pbir/constantly-fn-resolver :foo ::foo)
   (pbir/constantly-resolver :false false)])

(deftest process-test
  (testing "read"
    (is (= (p.eql/process (pci/register registry)
                          [::coords])
           {::coords
            [{:x 10 :y 20}
             {::geo/left 20 ::geo/width 5}]}))

    (is (= (p.eql/process (pci/register geo/full-registry)
                          {:left 10}
                          [::geo/x])
           {::geo/x 10}))

    (testing "when not found, key is omitted"
      (is (= (p.eql/process (-> (pci/register
                                  {:com.wsscode.pathom3.error/lenient-mode? true}
                                  geo/full-registry)
                                (p.ent/with-entity {:left 10}))
                            [::geo/top])
             {:com.wsscode.pathom3.connect.runner/attribute-errors {:com.wsscode.pathom3.test.geometry-resolvers/top {:com.wsscode.pathom3.error/cause :com.wsscode.pathom3.error/attribute-unreachable}}}))))

  (testing "reading with *"
    (is (= (-> (p.eql/process (-> (pci/register geo/full-registry)
                                  (p.ent/with-entity {:left 10}))
                              [::geo/x '*]))
           {::geo/x    10
            ::geo/left 10
            :left      10})))

  (testing "nested read"
    (is (= (p.eql/process (-> (pci/register geo/full-registry)
                              (p.ent/with-entity {:left 10 :top 5}))
                          [{::geo/turn-point [:right]}])
           {::geo/turn-point {:right 10}}))

    (is (= (p.eql/process (-> (pci/register geo/full-registry)
                              (p.ent/with-entity {:foo        {::geo/x 10}
                                                  :bar        {::geo/y 4
                                                               :mess   "here"}
                                                  ::geo/width 50
                                                  :other      "value"}))
                          [{:foo [:x]}
                           {:bar [:top]}
                           :width])
           {:foo   {:x 10}
            :bar   {:top 4}
            :width 50})))

  (testing "process sequence"
    (is (= (p.eql/process (-> {:com.wsscode.pathom3.error/lenient-mode? true}
                              (pci/register registry)
                              (p.ent/with-entity {::coords (list
                                                             {:x 10 :y 20}
                                                             {::geo/left 20 ::geo/width 5})}))
                          [{::coords [:right]}])
           {::coords [{:com.wsscode.pathom3.connect.runner/attribute-errors {:right {:com.wsscode.pathom3.error/cause :com.wsscode.pathom3.error/attribute-unreachable}}}
                      {:right 25}]})))

  (testing "process vector"
    (let [res (p.eql/process (-> (pci/register registry)
                                 (p.ent/with-entity {::coords [{::geo/left 20 ::geo/width 15}
                                                               {::geo/left 20 ::geo/width 5}]}))
                             [{::coords [:right]}])]
      (is (= res {::coords [{:right 35} {:right 25}]}))
      (is (vector? (::coords res)))))

  (testing "process set"
    (is (= (p.eql/process (-> (pci/register registry)
                              (p.ent/with-entity {::coords #{{::geo/left 20 ::geo/width 15}
                                                             {::geo/left 20 ::geo/width 5}}}))
                          [{::coords [:right]}])
           {::coords #{{:right 35} {:right 25}}}))))

(deftest process-one-test
  (is (= (p.eql/process-one (pci/register registry) {:left 10 :right 30} :width)
         20))

  (is (= (p.eql/process-one (pci/register geo/full-registry)
                            {:left 10 :top 5}
                            {::geo/turn-point [:right]})
         {:right 10}))

  (testing "keeps meta"
    (let [response (p.eql/process-one
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
      (let [response (p.eql/process-one
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
      (is (= (p.eql/process-one (pci/register registry) :false)
             false)))))

(defn run-boundary-interface [env request]
  (let [fi (p.eql/boundary-interface env)]
    (fi request)))

(deftest boundary-interface-test
  (let [fi (p.eql/boundary-interface (pci/register registry))]
    (testing "call with just tx"
      (is (= (fi [::coords])
             {::coords
              [{:x 10 :y 20}
               {::geo/left 20 ::geo/width 5}]})))

    (testing "call with ast"
      (is (= (fi {:pathom/ast (eql/query->ast [::coords])})
             {::coords
              [{:x 10 :y 20}
               {::geo/left 20 ::geo/width 5}]})))

    (testing "call with entity and eql"
      (is (= (fi {:pathom/entity {:left 10}
                  :pathom/eql    [:x]})
             {:x 10})))

    (testing "call with entity and tx"
      (is (= (fi {:pathom/entity {:left 10}
                  :pathom/tx     [:x]})
             {:x 10})))

    (testing "merge env"
      (is (= (fi [:foo])
             {:foo nil}))

      (is (= (fi {::foo "bar"} [:foo])
             {:foo "bar"})))

    (testing "modify env"
      (is (= (fi #(pci/register % (pbir/constantly-resolver :new "value")) [:new])
             {:new "value"})))

    (testing "error reporting"
      (check (=>
               {:com.wsscode.pathom3.error/error-data        {:error/code :err}
                :com.wsscode.pathom3.error/error-message     "Resolver error exception at path []: Err",
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
                       (throw (ex-info "Err" {:error/code :err})))))
                 {:pathom/eql [:error]})))

      (testing "partial success"
        (check (=>
                 {:com.wsscode.pathom3.error/error-data        {:error/code :err}
                  :com.wsscode.pathom3.error/error-message     "Resolver error exception at path []: Err",
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
                          (throw (ex-info "Err" {:error/code :err}))))])
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
               :com.wsscode.pathom3.error/error-data
               {:error/code :err}
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
                       (throw (ex-info "Err" {:error/code :err}))))])
                {:pathom/eql    [{:foo [:error]}]
                 :pathom/entity {:foo {:x 10}}})))))

    (testing "lenient mode"
      (is (= (fi {:pathom/eql           [:invalid]
                  :pathom/lenient-mode? true})
             {:com.wsscode.pathom3.connect.runner/attribute-errors {:invalid {:com.wsscode.pathom3.error/cause :com.wsscode.pathom3.error/attribute-unreachable}}}))

      (testing "lenient mode from env"
        (let [fi (p.eql/boundary-interface (assoc (pci/register registry) :com.wsscode.pathom3.error/lenient-mode? true))]
          (is (= (fi {:pathom/eql [:invalid]})
                 {:com.wsscode.pathom3.connect.runner/attribute-errors {:invalid {:com.wsscode.pathom3.error/cause :com.wsscode.pathom3.error/attribute-unreachable}}})))))))

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
