(ns com.wsscode.pathom3.connect.foreign-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.foreign :as pcf]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.operation.transit :as pct]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]
    #?(:clj [com.wsscode.pathom3.interface.async.eql :as p.a.eql])
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [com.wsscode.pathom3.test.helpers :as h]
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

(deftest compute-foreign-query-with-cycles-test
  (testing "basic impossible nested input path"
    (let [foreign (-> (pci/register
                        [(pco/resolver 'parent
                           {::pco/output [{:parent [:foo]}]}
                           (fn [_ _]))
                         (pco/resolver 'child
                           {::pco/input  [{:parent [:child]}]
                            ::pco/output [:child]}
                           (fn [_ _]))])
                      (serialize-boundary))
          env     (-> (pci/register
                        [(pcf/foreign-register foreign)]))]
      (is
        (thrown-with-msg?
          #?(:clj Throwable :cljs :default)
          #"Error while processing request \[:child] for entity \{}"
          (p.eql/process env [:child])))))

  (testing "indirect cycle"
    (let [foreign (-> (pci/register
                        [(pco/resolver 'parent
                           {::pco/output [{:parent [:foo]}]}
                           (fn [_ _]))
                         (pco/resolver 'child
                           {::pco/input  [{:parent [:child-dep]}]
                            ::pco/output [:child]}
                           (fn [_ _]))
                         (pco/resolver 'child-dep
                           {::pco/input  [:child]
                            ::pco/output [:child-dep]}
                           (fn [_ _]))])
                      (serialize-boundary))
          env     (-> (pci/register
                        [(pcf/foreign-register foreign)]))]
      (is
        (thrown-with-msg?
          #?(:clj Throwable :cljs :default)
          #"Error while processing request \[:child] for entity \{}"
          (p.eql/process env [:child])))))

  (testing "deep cycle"
    (let [foreign (-> (pci/register
                        [(pco/resolver 'parent
                           {::pco/output [{:parent [:foo]}]}
                           (fn [_ _]))
                         (pco/resolver 'child
                           {::pco/input  [{:parent [{:parent [:child]}]}]
                            ::pco/output [:child]}
                           (fn [_ _]))])
                      (serialize-boundary))
          env     (-> (pci/register
                        [(pcf/foreign-register foreign)]))]

      (is
        (thrown-with-msg?
          #?(:clj Throwable :cljs :default)
          #"Error while processing request \[:child] for entity \{}"
          (p.eql/process env [:child]))))))

(deftest compute-foreign-query-test
  (testing "no inputs"
    (is (= (pcf/compute-foreign-request
             [{::pcp/foreign-ast (eql/query->ast [:a])}])
           {:pathom/ast {:type :root,
                         :children [{:type :join,
                                     :key :com.wsscode.pathom3.connect.foreign/foreign-0,
                                     :dispatch-key :com.wsscode.pathom3.connect.foreign/foreign-0,
                                     :children [{:type :prop, :dispatch-key :a, :key :a}]}]},
            :pathom/entity {:com.wsscode.pathom3.connect.foreign/foreign-0 {}}})))

  (testing "inputs, but no parent ident, single attribute always goes as ident"
    (is (= (pcf/compute-foreign-request
             [{::pcp/foreign-ast         (eql/query->ast [:a])
               ::pcr/node-resolver-input {:z "bar"}}])
           {:pathom/ast {:type :root,
                         :children [{:type :join,
                                     :key :com.wsscode.pathom3.connect.foreign/foreign-0,
                                     :dispatch-key :com.wsscode.pathom3.connect.foreign/foreign-0,
                                     :children [{:type :prop, :dispatch-key :a, :key :a}]}]},
            :pathom/entity {:com.wsscode.pathom3.connect.foreign/foreign-0 {:z "bar"}}})))

  (testing "with multiple inputs"
    (is (= (pcf/compute-foreign-request
             [{::pcp/foreign-ast         (eql/query->ast [:a])
               ::pcr/node-resolver-input {:x "foo"
                                          :z "bar"}}])
           {:pathom/ast {:type :root,
                         :children [{:type :join,
                                     :key :com.wsscode.pathom3.connect.foreign/foreign-0,
                                     :dispatch-key :com.wsscode.pathom3.connect.foreign/foreign-0,
                                     :children [{:type :prop, :dispatch-key :a, :key :a}]}]},
            :pathom/entity {:com.wsscode.pathom3.connect.foreign/foreign-0 {:x "foo", :z "bar"}}}))))

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

  (testing "multiple inputs"
    (let [foreign (-> (pci/register [(pbir/single-attr-resolver :a :aa str)
                                     (pbir/single-attr-resolver :b :bb str)])
                      (serialize-boundary))
          env     (-> (pci/register
                        [(pco/resolver 'ab {::pco/output [:a :b]} (fn [_ _] {:a 1 :b 2}))
                         (pcf/foreign-register foreign)]))]
      (is (= (p.eql/process env [:aa :bb])
             {:aa "1" :bb "2"}))))

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
               {:a {:c "value"}}))))

    (testing "nested inputs"
      (let [foreign (-> (pci/register
                          (pco/resolver 'n
                            {::pco/output [{:a [:b]}]}
                            (fn [_ _] {:a [{:b 1}
                                           {:b 2}
                                           {:b 3}]})))
                        (serialize-boundary))
            env     (-> (pci/register
                          [(pco/resolver 'sum
                             {::pco/input
                              [{:a [:b]}]

                              ::pco/output
                              [:total]}
                             (fn [_ {:keys [a]}]
                               {:total (transduce (map :b) + 0 a)}))
                           (pcf/foreign-register foreign)]))]
        (is (= (p.eql/process env [:total])
               {:total 6})))))

  (testing "union queries"
    (let [foreign (-> (pci/register
                        [(pbir/global-data-resolver
                           {:list
                            [{:user/id 123}
                             {:video/id 2}]})
                         (pbir/static-attribute-map-resolver :user/id :user/name
                           {123 "U"})
                         (pbir/static-attribute-map-resolver :video/id :video/title
                           {2 "V"})])
                      (serialize-boundary))
          env     (-> (pci/register
                        [(pbir/constantly-resolver :y 20)
                         (pcf/foreign-register foreign)]))]
      (is (= (p.eql/process env [{:list
                                  {:user/id  [:user/name]
                                   :video/id [:video/title]}}])
             {:list [{:user/name "U"} {:video/title "V"}]}))))

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

(deftest process-foreign-query-batch
  (let [foreign (-> (pci/register (pbir/single-attr-resolver :x :y inc))
                    (serialize-boundary)
                    h/spy-fn)
        env     (-> (pci/register
                      [(pcf/foreign-register foreign)]))]
    (is (= (p.eql/process env
                          {:items
                           [{:x 1}
                            {:x 2}
                            {:x 3}]}
                          [{:items [:x :y]}])

           {:items
            [{:x 1
              :y 2}
             {:x 2
              :y 3}
             {:x 3
              :y 4}]}))

    (is (= (-> foreign meta :calls deref count)
           2))))

(deftest process-foreign-mutation-test
  (testing "basic foreign mutation call"
    (let [foreign (-> (pci/register (pco/mutation 'doit {::pco/output [:done]} (fn [_ _] {:done true})))
                      (serialize-boundary))
          env     (-> (pci/register (pcf/foreign-register foreign)))]
      (is (= (p.eql/process env ['(doit {})])
             {'doit {:done true}}))))

  #?(:clj
     (let [foreign (-> (pci/register (pco/mutation 'doit {::pco/output [:done]} (fn [_ _] {:done true})))
                       (serialize-boundary))
           env     (-> (pci/register (pcf/foreign-register foreign)))]
       (is (= @(p.a.eql/process env ['(doit {})])
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
