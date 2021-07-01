(ns com.wsscode.pathom3.connect.foreign-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.foreign :as pcf]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.operation.transit :as pct]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    #?(:clj [com.wsscode.pathom3.interface.async.eql :as p.a.eql])
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [com.wsscode.pathom3.path :as p.path]
    [com.wsscode.transito :as transito]
    [edn-query-language.core :as eql]
    [promesa.core :as p]))

(defn tread [s]
  (transito/read-str s {:handlers pct/read-handlers}))

(defn twrite [x]
  (transito/write-str x {:handlers pct/write-handlers}))

(defn write-read [x]
  (-> x twrite tread))

(defn serialize-boundary
  "Encode and decode request with transit to simulate wire process."
  [env]
  (let [boundary (p.eql/boundary-interface env)]
    (fn [request]
      (-> request write-read boundary write-read))))

(deftest compute-foreign-query-test
  (testing "no inputs"
    (is (= (pcf/compute-foreign-request
             (-> {::pcp/node {::pcp/foreign-ast (eql/query->ast [:a])}}
                 (p.ent/with-entity {})))
           #:pathom{:ast    {:children [{:dispatch-key :a
                                         :key          :a
                                         :type         :prop}]
                             :type     :root}
                    :entity {}})))

  (testing "inputs, but no parent ident, single attribute always goes as ident"
    (is (= (pcf/compute-foreign-request
             (-> {::pcp/node {::pcp/foreign-ast (eql/query->ast [:a])
                              ::pcp/input       {:z {}}}}
                 (p.ent/with-entity {:z "bar"})))
           #:pathom{:ast    {:children [{:dispatch-key :a
                                         :key          :a
                                         :type         :prop}]
                             :type     :root}
                    :entity {:z "bar"}})))

  (testing "with multiple inputs"
    (is (= (pcf/compute-foreign-request
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
                      (serialize-boundary))
          env     (-> (pci/register
                        [(pbir/constantly-resolver :y 20)
                         (pcf/foreign-register foreign)]))]
      (is (= (p.eql/process env [:x :y])
             {:x 10 :y 20})))

    (let [foreign  (-> (pci/register
                         [(pbir/single-attr-resolver :a :b inc)
                          (pbir/single-attr-resolver :c :d inc)
                          (pbir/single-attr-resolver :e :f inc)])
                       (serialize-boundary))
          foreign2 (-> (pci/register
                         [(pbir/single-attr-resolver :g :h inc)])
                       (serialize-boundary))
          env      (-> (pci/register
                         [(pcf/foreign-register foreign)
                          (pcf/foreign-register foreign2)
                          (pbir/single-attr-resolver :b :c inc)
                          (pbir/single-attr-resolver :d :e inc)
                          (pbir/single-attr-resolver :f :g inc)]))]
      (is (= (p.eql/process env {:a 1} [:h])
             {:h 8}))))

  (testing "nested query"
    (testing "direct nest"
      (let [foreign (-> (pci/register
                          (pco/resolver 'n
                            {::pco/output [{:a [:b :c]}]}
                            (fn [_ _] {:a {:b 1 :c 2}})))
                        (serialize-boundary))
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
                        (serialize-boundary))
            env     (-> (pci/register
                          [(pbir/alias-resolver :b :c)
                           (pcf/foreign-register foreign)]))]
        (is (= (p.eql/process env [{:a [:c]}])
               {:a {:c "value"}})))))

  #?(:clj
     (testing "async foreign"
       (let [foreign (-> (pci/register (pbir/constantly-resolver :x 10))
                         (p.a.eql/boundary-interface))
             env     (p/let [f' (pcf/foreign-register foreign)]
                       (-> (pci/register
                             [(pbir/constantly-resolver :y 20)
                              f'])))]
         (is (= @(p.a.eql/process env [:x :y])
                {:x 10 :y 20}))))))

(deftest process-foreign-mutation-test
  (testing "basic foreign mutation call"
    (let [foreign (-> (pci/register (pco/mutation 'doit {::pco/output [:done]} (fn [_ _] {:done true})))
                      (serialize-boundary))
          env     (-> (pci/register (pcf/foreign-register foreign)))]
      (is (= (p.eql/process env ['(doit {})])
             {'doit {:done true}}))))

  (testing "mutation query going over"
    (testing "attribute directly in mutation output"
      (let [foreign (-> (pci/register
                          (pco/mutation 'doit {::pco/output [:done]} (fn [_ _] {:done true :other "bla"})))
                        (serialize-boundary))
            env     (-> (pci/register (pcf/foreign-register foreign)))]
        (is (= (p.eql/process env [{'(doit {}) [:done]}])
               {'doit {:done true}}))))

    (testing "attribute extended from mutation, but still in the same foreign"
      (let [foreign (-> (pci/register
                          [(pbir/alias-resolver :done :done?)
                           (pco/mutation 'doit {::pco/output [:done]} (fn [_ _] {:done true :other "bla"}))])
                        (serialize-boundary))
            env     (-> (pci/register (pcf/foreign-register foreign)))]
        (is (= (p.eql/process env [{'(doit {}) [:done?]}])
               {'doit {:done? true}}))))

    (testing "attribute extended from mutation locally"
      (let [foreign (-> (pci/register
                          (pco/mutation 'doit {::pco/output [:done]} (fn [_ _] {:done true :other "bla"})))
                        (serialize-boundary))
            env     (-> (pci/register
                          [(pcf/foreign-register foreign)
                           (pbir/alias-resolver :done :done?)]))]
        (is (= (p.eql/process env [{'(doit {}) [:done?]}])
               {'doit {:done? true}}))))))

(comment
  (let [foreign (-> (pci/register (pbir/constantly-resolver :x 10))
                    (serialize-boundary))
        env     (-> (pci/register
                      [(pbir/constantly-resolver :y 20)
                       (pcf/foreign-register foreign)]))]
    (pcf/foreign-register foreign))

  (let [foreign (-> (pci/register
                      (pco/resolver 'n
                        {::pco/output [{:a [:b :c]}]}
                        (fn [_ _] {:a {:b 1 :c 2}})))
                    (p.eql/boundary-interface))
        env     (-> (pci/register
                      [(pbir/constantly-resolver :y 20)
                       (pcf/foreign-register foreign)])
                    ((requiring-resolve 'com.wsscode.pathom.viz.ws-connector.pathom3/connect-env)
                     "debug"))]

    (p.eql/process env [{:a [:b]}]))

  (let [foreign (-> (pci/register (pbir/constantly-resolver :x 10))
                    (p.eql/boundary-interface))
        env     (-> (pci/register
                      [(pbir/constantly-resolver :y 20)
                       (pcf/foreign-register foreign)]))]
    env)

  ((requiring-resolve 'com.wsscode.pathom.viz.ws-connector.pathom3/log-entry)
   {:pathom.viz.log/type :pathom.viz.log.type/trace
    :pathom.viz.log/data *1}))
