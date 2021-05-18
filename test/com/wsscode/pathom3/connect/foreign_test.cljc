(ns com.wsscode.pathom3.connect.foreign-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.foreign :as pcf]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.interface.eql :as p.eql]
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
           #:pathom{:ast    {:children [{:dispatch-key :a
                                         :key          :a
                                         :type         :prop}]
                             :type     :root}
                    :entity {}})))

  (testing "inputs, but no parent ident, single attribute always goes as ident"
    (is (= (pcf/compute-foreign-query
             (-> {::pcp/node {::pcp/foreign-ast (eql/query->ast [:a])
                              ::pcp/input       {:z {}}}}
                 (p.ent/with-entity {:z "bar"})))
           #:pathom{:ast    {:children [{:dispatch-key :a
                                         :key          :a
                                         :type         :prop}]
                             :type     :root}
                    :entity {:z "bar"}})))

  (testing "with multiple inputs"
    (is (= (pcf/compute-foreign-query
             (-> {::pcp/node    {::pcp/foreign-ast (eql/query->ast [:a])
                                 ::pcp/input       {:x {}
                                                    :z {}}}
                  ::p.path/path [[:z "bar"] :a]}
                 (p.ent/with-entity {:x "foo"
                                     :z "bar"})))
           #:pathom{:ast    {:children [{:dispatch-key :a
                                         :key          :a
                                         :type         :prop}]
                             :type     :root}
                    :entity {:x "foo"
                             :z "bar"}}))))

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

(deftest process-foreign-query
  (testing "basic integration"
    (let [foreign (-> (pci/register (pbir/constantly-resolver :x 10))
                      (p.eql/foreign-interface))
          env     (-> (pci/register
                        [(pbir/constantly-resolver :y 20)
                         (pcf/foreign-register foreign)]))]
      (is (= (p.eql/process env [:x :y])
             {:x 10 :y 20}))))

  (testing "nested query"
    (testing "direct nest"
      (let [foreign (-> (pci/register
                          (pco/resolver 'n
                            {::pco/output [{:a [:b :c]}]}
                            (fn [_ _] {:a {:b 1 :c 2}})))
                        (p.eql/foreign-interface))
            env     (-> (pci/register
                          [(pbir/constantly-resolver :y 20)
                           (pcf/foreign-register foreign)]))]
        (is (= (p.eql/process env [:a])
               {:a {:b 1 :c 2}}))

        (is (= (p.eql/process env [{:a [:b]}])
               {:a {:b 1}}))))

    (testing "extended deps"
      (let [foreign (-> (pci/register
                          (pco/resolver 'n
                            {::pco/output [{:a [:b]}]}
                            (fn [_ _] {:a {:b "value"}})))
                        (p.eql/foreign-interface))
            env     (-> (pci/register
                          [(pbir/constantly-resolver :y 20)
                           (pcf/foreign-register foreign)]))]
        (is (= (p.eql/process env [:a])
               {:a {:b "value"}}))))))

(comment
  (let [foreign (-> (pci/register (pbir/constantly-resolver :x 10))
                    (p.eql/foreign-interface))
        env     (-> (pci/register
                      [(pbir/constantly-resolver :y 20)
                       (pcf/foreign-register foreign)]))]
    (pcf/foreign-register foreign))

  (let [foreign (-> (pci/register
                      (pco/resolver 'n
                        {::pco/output [{:a [:b :c]}]}
                        (fn [_ _] {:a {:b 1 :c 2}})))
                    (p.eql/foreign-interface))
        env     (-> (pci/register
                      [(pbir/constantly-resolver :y 20)
                       (pcf/foreign-register foreign)])
                    ((requiring-resolve 'com.wsscode.pathom.viz.ws-connector.pathom3/connect-env)
                     "debug"))]

    (p.eql/process env [{:a [:b]}]))

  (let [foreign (-> (pci/register (pbir/constantly-resolver :x 10))
                    (p.eql/foreign-interface))
        env     (-> (pci/register
                      [(pbir/constantly-resolver :y 20)
                       (pcf/foreign-register foreign)]))]
    (p.eql/process env [:x :y]))

  ((requiring-resolve 'com.wsscode.pathom.viz.ws-connector.pathom3/log-entry)
   {:pathom.viz.log/type :pathom.viz.log.type/trace
    :pathom.viz.log/data *1}))
