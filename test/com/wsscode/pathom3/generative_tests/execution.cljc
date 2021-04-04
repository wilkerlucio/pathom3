(ns com.wsscode.pathom3.generative-tests.execution
  (:require
    ;[clojure.test :refer [deftest is are run-tests testing]]
    [clojure.test.check :as tc]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as prop]
    [com.wsscode.pathom.connect :as pc]
    [com.wsscode.pathom.core :as p]
    [com.wsscode.pathom.viz.ws-connector.pathom3 :as p.connector]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [edn-query-language.core :as eql]))

#_ :clj-kondo/ignore

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

   ::gen-dep-resolver-dependency
   (fn [{::p.attr/keys [attribute]
         ::keys        [gen-resolver attributes]
         :as           env}]
     (if (seq attributes)
       (gen/one-of
         [(gen-resolver
            (assoc env
              ::p.attr/attribute attribute))
          (gen/let [attr (gen/elements attributes)]
            (gen/return
              {::resolvers  []
               ::query      [attr]
               ::expected   {attr (name attr)}
               ::attributes #{attr}}))])
       (gen-resolver
         (assoc env
           ::p.attr/attribute attribute))))

   ::gen-dep-resolver
   (fn [{::p.attr/keys [attribute]
         ::keys        [max-deps gen-dep-resolver-dependency]
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
                         #(gen-dep-resolver-dependency
                            (assoc env
                              ::p.attr/attribute %))
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

(defn p3-resolver->p2-resolver [{::pco/keys [op-name input output resolve]}]
  {::pc/sym     op-name
   ::pc/input   (set input)
   ::pc/output  output
   ::pc/resolve resolve})

(defn runner-p2
  [{::keys [resolvers query]}]
  (let [parser (p/parser
                 {::p/env     {::p/reader               [p/map-reader
                                                         pc/reader2
                                                         pc/open-ident-reader
                                                         p/env-placeholder-reader]
                               ::p/placeholder-prefixes #{">"}}
                  ::p/mutate  pc/mutate
                  ::p/plugins [(pc/connect-plugin {::pc/register (mapv p3-resolver->p2-resolver resolvers)})
                               p/error-handler-plugin
                               p/trace-plugin]})]
    (parser {} query)))

(defn runner-p3
  [{::keys [resolvers query]}]
  (let [env (pci/register (mapv pco/resolver resolvers))]
    (p.eql/process env query)))

(defn generate-prop
  [runner env]
  (prop/for-all [{::keys [expected] :as case}
                 ((::gen-request gen-env)
                  (merge gen-env env))]
    (= expected (runner case))))

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

(defn single-dep-prop [runner]
  (generate-prop
    runner
    {::max-resolver-depth
     5

     ::max-deps
     1

     ::max-request-attributes
     10}))

(defn complex-deps-prop [runner]
  (generate-prop
    runner
    {::max-resolver-depth
     2

     ::max-deps
     2

     ::max-request-attributes
     2}))

(defn result-smallest [result]
  (-> result
      :shrunk
      :smallest
      first))

(defn check-smallest [n prop]
  (-> (tc/quick-check n prop)
      result-smallest))

#_ :clj-kondo/ignore

(comment
  (runner-p3 fail)

  (log-request-snapshots fail)
  (log-request-snapshots fail2)

  (run-query-on-pathom-viz fail)
  (run-query-on-pathom-viz fail2)

  (runner-p2 fail)

  (pci/register (mapv pco/resolver (::resolvers fail)))

  (check-smallest 10000 (single-dep-prop runner-p2))
  (check-smallest 10000 (single-dep-prop runner-p3))

  (check-smallest 10000 (complex-deps-prop runner-p2))
  (check-smallest 10000 (complex-deps-prop runner-p3))

  (let [res (tc/quick-check 10000
              (generate-prop
                runner-p3
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

  (let [res (tc/quick-check 10000
              (generate-prop
                runner-p2
                {::max-resolver-depth
                 4

                 ::max-deps
                 2

                 ::max-request-attributes
                 10}))]
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
  )
