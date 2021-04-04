(ns com.wsscode.pathom3.generative-tests.planner
  (:require
    [clojure.spec.alpha :as s]
    [clojure.spec.test.alpha :as s.test]
    [clojure.test :refer [deftest is are run-tests testing]]
    [clojure.test.check :as tc]
    [clojure.test.check.clojure-test :as test]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as props]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom.viz.ws-connector.pathom3 :as p.connector]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [edn-query-language.core :as eql]
    [edn-query-language.gen :as eql-gen]))

(defn next-attr [attribute]
  (let [[base current-index]
        (or (some->
              (re-find #"(.+)(\d+)$" (name attribute))
              (->> (drop 1))
              (vec)
              (update 1 #(Long/parseLong %)))
            [(name attribute) 0])]
    (keyword (str base (inc current-index)))))

(def base-chars
  [:a :b :c :d :e :f :g :h :i :j :k :l :m
   :n :o :p :q :r :s :t :u :v :x :y :w :z])

(comment
  (next-attr :a4214)
  (take 10 (iterate next-attr :a)))

(defn merge-result [r1 r2]
  {::resolvers  (into (::resolvers r1) (::resolvers r2))
   ::query      (into (::query r1) (::query r2))
   ::expected   (merge (::expected r1) (::expected r2))
   ::attributes (into (::attributes r1) (::attributes r2))})

(def blank-result
  {::resolvers  []
   ::query      []
   ::expected   {}
   ::attributes #{}})

(def gen-env
  {::gen-root-resolver
   (fn [{::p.attr/keys [attribute]}]
     (gen/return
       {::resolvers [{::pco/op-name (symbol attribute)
                      ::pco/output  [attribute]
                      ::pco/resolve (fn [_ _]
                                      {attribute (name attribute)})}]
        ::query     [attribute]
        ::expected  {attribute (name attribute)}}))

   ::gen-chained-resolver
   (fn [{::p.attr/keys [attribute]
         ::keys        [gen-resolver-request]
         :as           env}]
     (let [next-attr (next-attr attribute)]
       (gen/let [next-group
                 (gen-resolver-request
                   (assoc env ::p.attr/attribute next-attr))]
         (gen/return
           {::resolvers  (conj (::resolvers next-group)
                           {::pco/op-name (symbol attribute)
                            ::pco/input   [next-attr]
                            ::pco/output  [attribute]
                            ::pco/resolve (fn [_ _]
                                            {attribute (name attribute)})})
            ::query      [attribute]
            ::expected   {attribute (name attribute)}
            ::attributes (into #{attribute} (::attributes next-group))}))))

   ::max-resolver-depth
   5

   ::gen-resolver-request
   (fn [{::keys [max-resolver-depth
                 gen-root-resolver
                 gen-dep-resolver] :as env}]
     (let [env (update env ::max-resolver-depth dec)]
       (if (pos? max-resolver-depth)
         (gen/one-of
           [(gen-root-resolver env)
            (gen-dep-resolver env)])
         (gen-root-resolver env))))})

(def gen-env
  {::p.attr/attribute
   :a

   ::used-attributes
   #{}

   ::max-resolver-depth
   5

   ::max-deps
   4

   ::max-request-attributes
   3

   ::gen-root-resolver
   (fn [{::p.attr/keys [attribute]}]
     (gen/return
       {::resolvers  [{::pco/op-name (symbol attribute)
                       ::pco/output  [attribute]
                       ::pco/resolve (fn [_ _]
                                       {attribute (name attribute)})}]
        ::query      [attribute]
        ::expected   {attribute (name attribute)}
        ::attributes #{attribute}}))

   ::gen-dep-resolver
   (fn [{::p.attr/keys [attribute]
         ::keys        [gen-resolver attributes max-deps]
         :as           env}]
     (let [next-attrs (->> (iterate next-attr attribute)
                           (drop 1)
                           (map #(keyword (str (name %) "-a"))))]
       (gen/let [deps-count
                 (gen/choose 1 max-deps)]
         (let [inputs (take deps-count next-attrs)]
           (gen/let [next-groups
                     (apply gen/tuple
                       (mapv
                         #(if (seq attributes)
                            (gen/one-of
                              [(gen-resolver
                                 (assoc env
                                   ::p.attr/attribute %))
                               (gen/let [attr (gen/elements attributes)]
                                 (gen/return
                                   {::resolvers  []
                                    ::query      [attr]
                                    ::expected   {attr (name attr)}
                                    ::attributes #{attr}}))])
                            (gen-resolver
                              (assoc env
                                ::p.attr/attribute %)))
                         inputs))]
             (gen/return
               {::resolvers  (into
                               [{::pco/op-name (symbol attribute)
                                 ::pco/input   (into [] (mapcat ::query) next-groups)
                                 ::pco/output  [attribute]
                                 ::pco/resolve (fn [_ _]
                                                 {attribute (name attribute)})}]
                               (mapcat ::resolvers)
                               next-groups)
                ::query      [attribute]
                ::expected   {attribute (name attribute)}
                ::attributes (reduce into #{attribute} (map ::attributes next-groups))}))))))

   ::gen-resolver
   (fn [{::keys [max-resolver-depth
                 gen-root-resolver
                 gen-dep-resolver] :as env}]
     (let [env (update env ::max-resolver-depth dec)]
       (if (pos? max-resolver-depth)
         (gen/frequency
           [[3 (gen-root-resolver env)]
            [2 (gen-dep-resolver env)]])
         (gen-root-resolver env))))

   ::gen-resolver-chain
   (fn [{::keys [gen-resolver
                 gen-resolver-chain
                 query-queue
                 chain-result]
         :as    env}]
     (let [[attr & rest] query-queue]
       (if attr
         (let [env (assoc env ::p.attr/attribute attr)]
           (gen/let [result (gen-resolver env)]
             (gen-resolver-chain
               (-> env
                   (assoc ::query-queue rest)
                   (update ::chain-result merge-result result)
                   (as-> <>
                     (assoc <>
                       ::attributes
                       (-> <> ::chain-result ::attributes)))))))
         (gen/return
           chain-result))))

   ::gen-request
   (fn [{::keys [gen-resolver-chain max-request-attributes] :as env}]
     (gen/let [items-count (gen/choose 1 max-request-attributes)]
       (let [query (vec (take items-count base-chars))]
         (gen-resolver-chain
           (assoc env
             ::query-queue query
             ::chain-result blank-result)))))})

(defn run-thing
  [{::keys [resolvers query expected]}]
  (let [env (pci/register (mapv pco/resolver resolvers))]
    (p.eql/process env query)))

(defn run-generated-test
  [{::keys [expected] :as request}]
  (let [res (run-thing request)]
    (if (= res expected)
      true
      (do
        (p.connector/log-entry
          #_{:pathom.viz.log/type  :pathom.viz.log.type/plan-and-stats
             :pathom.viz.log/value (:com.wsscode.pathom3.connect.runner/run-stats (meta res))}
          (assoc (:com.wsscode.pathom3.connect.runner/run-stats (meta res))
            :pathom.viz.log/type :pathom.viz.log.type/plan-and-stats))
        false))))

(defn generate-prop [env]
  (props/for-all [case ((::gen-request gen-env)
                        (merge gen-env env))]
                 (run-generated-test case)))

(defn log-request-snapshots [req]
  (p.connector/log-entry
    {:pathom.viz.log/type :pathom.viz.log.type/plan-snapshots
     :pathom.viz.log/data (pcp/compute-plan-snapshots
                            (-> (pci/register (mapv pco/resolver (::resolvers req)))
                                (assoc :edn-query-language.ast/node
                                  (eql/query->ast (::query req)))))}))

(defn run-query-on-pathom-viz [req]
  (-> (pci/register (mapv pco/resolver (::resolvers req)))
      (p.connector/connect-env
        {:com.wsscode.pathom.viz.ws-connector.core/parser-id "debug"})
      (p.eql/process (::query req))))

(comment
  (run-thing
    fail)

  (log-request-snapshots fail)
  (log-request-snapshots fail2)

  (run-query-on-pathom-viz fail)
  (run-query-on-pathom-viz fail2)

  (let [res (tc/quick-check 1000
              (generate-prop
                {::max-resolver-depth
                 2

                 ::max-deps
                 2

                 ::max-request-attributes
                 2}))]
    (-> res
        :shrunk
        :smallest
        first))

  (def fail *1)
  (def fail2 *1)
  (gen/sample
    ((::gen-resolver gen-env)
     gen-env))

  (gen/generate
    ((::gen-request gen-env)
     gen-env))

  (dotimes [_ 20]
    (run-generated-test
      (gen/generate
        ((::gen-request gen-env)
         gen-env)))
    (Thread/sleep 100))
  )
