(ns com.wsscode.pathom3.generative-tests.planner
  (:require
    [clojure.spec.alpha :as s]
    [clojure.spec.test.alpha :as s.test]
    [clojure.test :refer [deftest is are run-tests testing]]
    [clojure.test.check :as tc]
    [clojure.test.check.clojure-test :as test]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as props]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [com.wsscode.pathom.viz.ws-connector.pathom3 :as p.connector]
    [edn-query-language.gen :as eql-gen]
    [com.wsscode.misc.coll :as coll]))

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
  {::resolvers (into (::resolvers r1) (::resolvers r2))
   ::query     (into (::query r1) (::query r2))
   ::expected  (merge (::expected r1) (::expected r2))})

(def blank-result
  {::resolvers []
   ::query     []
   ::expected  {}})

(def gen-env
  {::p.attr/attribute
   :a

   ::used-attributes
   #{}

   ::max-resolver-depth
   5

   ::gen-root-resolver
   (fn [{::p.attr/keys [attribute]}]
     (gen/return
       {::resolvers [{::pco/op-name (symbol attribute)
                      ::pco/output  [attribute]
                      ::pco/resolve (fn [_ _]
                                      {attribute (name attribute)})}]
        ::query     [attribute]
        ::expected  {attribute (name attribute)}}))

   #_#_::gen-root-blank-resolver
       (fn [{::p.attr/keys [attribute]}]
         (gen/return
           {::resolvers [{::pco/op-name (symbol attribute)
                          ::pco/output  [attribute]
                          ::pco/resolve (fn [_ _]
                                          {})}]
            ::query     [attribute]
            ::expected  {}}))

   ::gen-dep-resolver
   (fn [{::p.attr/keys [attribute]
         ::keys        [gen-resolver]
         :as           env}]
     (let [next-attrs (->> (iterate next-attr attribute)
                           (drop 1)
                           (map #(keyword (str (name %) "-a"))))]
       (gen/let [deps-count
                 (gen/choose 1 4)

                 next-groups
                 (apply gen/tuple
                   (mapv
                     #(gen-resolver
                        (assoc env
                          ::p.attr/attribute %))
                     (take deps-count next-attrs)))]
         (gen/return
           {::resolvers (into
                          [{::pco/op-name (symbol attribute)
                            ::pco/input   (vec (take deps-count next-attrs))
                            ::pco/output  [attribute]
                            ::pco/resolve (fn [_ _]
                                            {attribute (name attribute)})}]
                          (mapcat ::resolvers)
                          next-groups)
            ::query     [attribute]
            ::expected  {attribute (name attribute)}}))))

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
                   (update ::chain-result merge-result result)))))
         (gen/return
           chain-result))))

   ::gen-request
   (fn [{::keys [gen-resolver-chain] :as env}]
     (gen/let [items-count (gen/choose 1 3)]
       (let [query (vec (take items-count base-chars))]
         (gen-resolver-chain
           (assoc env
             ::query-queue query
             ::chain-result blank-result)))))})

(defn run-generated-test
  [{::keys [resolvers query expected]}]
  (let [env (pci/register (mapv pco/resolver resolvers))
        res (p.eql/process env query)]
    (p.connector/log-entry
      #_{:pathom.viz.log/type  :pathom.viz.log.type/plan-and-stats
         :pathom.viz.log/value (:com.wsscode.pathom3.connect.runner/run-stats (meta res))}
      (assoc (:com.wsscode.pathom3.connect.runner/run-stats (meta res))
        :pathom.viz.log/type :pathom.viz.log.type/plan-and-stats))
    (if (= res expected)
      true
      (do

        false))))

(def generate-prop
  (props/for-all [case ((::gen-resolver gen-env)
                        gen-env)]
    (run-generated-test case)))

(comment
  (tc/quick-check 1000 generate-prop)
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

  (run-generated-test
    {::resolvers
     [{::pco/op-name 'a
       ::pco/output  [:a]
       ::pco/resolve (fn [_ _] {:a "a"})}]

     ::query
     [:a]

     ::expected
     {:a "a"}})

  (run-generated-test
    {::resolvers
     [{::pco/op-name 'a
       ::pco/output  [:a]
       ::pco/resolve (fn [_ _])}]

     ::query
     [:a]

     ::expected
     {}})

  (run-generated-test
    {::resolvers
     []

     ::query
     [:a]

     ::expected
     {}})

  (let [attribute :a]
    (gen/generate
      (gen/let [resolvers
                (gen/hash-map
                  ::pco/op-name (symbol))]
        )))

  (gen/sample
    (gen/vector-distinct (s/gen #{:a :b :c :d :e})))



  (let [attribute-spectrum]
    (gen/generate
      (gen/let [attribute-spectrum
                (gen/set
                  (gen/elements [:a :b :c :d :e]))

                attribute-resolutions
                (gen/fmap
                  (fn [])
                  attribute-spectrum)

                resolvers
                (gen/hash-map
                  )

                #_#_query
                    (gen/generate
                      (eql-gen/make-gen
                        {::eql-gen/gen-query-expr
                         (fn gen-query-expr [{::eql-gen/keys [gen-property] :as env}]
                           (gen-property env))

                         ::eql-gen/gen-query
                         (fn gen-query [{::eql-gen/keys [gen-property gen-query-expr gen-max-depth] :as env}]
                           (gen/vector-distinct (gen-property env)
                             {:min-elements 0 :max-elements 5}))}

                        ::eql-gen/gen-query))]
        ))))
