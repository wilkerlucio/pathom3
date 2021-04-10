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

#_:clj-kondo/ignore

(defn next-attr [attribute]
  (let [[base current-index]
        (or (some->
              (re-find #"(.+)(\d+)$" (name attribute))
              (->> (drop 1))
              (vec)
              (update 1 #(Long/parseLong %)))
            [(name attribute) 0])]
    (keyword (str base (inc current-index)))))

(defn gen-one-of
  "Like gen/one-of, but automatically removes nils."
  [generators]
  (gen/one-of (filterv some? generators)))

(defn gen-frequency
  "Like gen/frequency, but automatically removes nil generators."
  [generators]
  (gen/frequency
    (filterv (comp some? second) generators)))

(def base-chars
  [:a :b :c :d :e :f :g :h :i :j :k :l :m
   :n :o :p :q :r :s :t :u :v :x :y :w :z])

(comment
  (next-attr :a4214)
  (take 10 (iterate next-attr :a)))

(defn attrs->expected [attrs]
  (zipmap attrs (map name attrs)))

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
   10

   ::max-deps
   5

   ::max-request-attributes
   8

   ::max-resolver-outputs
   10

   ::knob-reuse-attributes?
   true

   ::gen-output-for-resolver
   (fn [{::p.attr/keys [attribute]
         ::keys        [max-resolver-outputs]}]
     (gen/let [extra-outputs
               (gen-frequency
                 [[3 (gen/return 0)]
                  [2 (if (pos? max-resolver-outputs)
                       (gen/choose 0 max-resolver-outputs))]])]
       (let [next-attrs (->> (iterate next-attr :ex1)
                             (map #(keyword (str (name attribute) "--" (name %))))
                             (take extra-outputs))]
         (into [attribute] next-attrs))))

   ::gen-resolver-no-deps
   (fn [{::p.attr/keys [attribute]
         ::keys        [gen-output-for-resolver]
         :as           env}]
     (gen/let [output (gen-output-for-resolver env)]
       {::resolvers  [{::pco/op-name (symbol attribute)
                       ::pco/output  output
                       ::pco/resolve (fn [_ _]
                                       (attrs->expected output))}]
        ::query      [attribute]
        ::expected   {attribute (name attribute)}
        ::attributes (set output)}))

   ; use some attribute generated before
   ::gen-resolver-reuse
   (fn [{::keys [attributes]}]
     (if (seq attributes)
       (gen/let [attr (gen/elements attributes)]
         {::resolvers  []
          ::query      [attr]
          ::expected   {attr (name attr)}
          ::attributes #{attr}})))

   ::gen-resolver-with-deps
   (fn [{::p.attr/keys [attribute]
         ::keys        [max-deps gen-resolver gen-output-for-resolver]
         :as           env}]
     (gen/let [deps-count (gen/choose 1 max-deps)
               output     (gen-output-for-resolver env)]
       (let [next-attrs (->> (iterate next-attr attribute)
                             (drop 1)
                             (map #(keyword (str (name %) "-a")))
                             (take deps-count))]
         (gen/let [next-groups
                   (apply gen/tuple
                     (mapv
                       #(gen-resolver
                          (assoc env
                            ::p.attr/attribute %))
                       next-attrs))]
           (let [actual-input (into []
                                    (comp (mapcat ::query)
                                          (distinct))
                                    next-groups)]
             {::resolvers  (into
                             [{::pco/op-name (symbol attribute)
                               ::pco/input   actual-input
                               ::pco/output  output
                               ::pco/resolve (fn [_ input]
                                               (assert (= input
                                                          (attrs->expected actual-input)))
                                               (attrs->expected output))}]
                             (mapcat ::resolvers)
                             next-groups)
              ::query      [attribute]
              ::expected   {attribute (name attribute)}
              ::attributes (reduce into (set output) (map ::attributes next-groups))})))))

   ::gen-resolver-leaf
   (fn [{::keys [gen-resolver-no-deps
                 gen-resolver-reuse
                 knob-reuse-attributes?] :as env}]
     (gen-one-of
       [(gen-resolver-no-deps env)
        (if knob-reuse-attributes?
          (gen-resolver-reuse env))]))

   ::gen-resolver
   (fn [{::keys [max-resolver-depth
                 gen-resolver-leaf
                 gen-resolver-with-deps] :as env}]
     (let [env (update env ::max-resolver-depth dec)]
       (if (pos? max-resolver-depth)
         (gen-frequency
           [[3 (gen-resolver-leaf env)]
            [2 (gen-resolver-with-deps env)]])
         (gen-resolver-leaf env))))

   ::gen-request-resolver-item
   (fn [{::keys [gen-resolver
                 gen-request-resolver-item
                 query-queue
                 chain-result]
         :as    env}]
     (let [[attr & rest] query-queue]
       (if attr
         (let [env (assoc env ::p.attr/attribute attr)]
           (gen/let [result (gen-resolver env)]
             (gen-request-resolver-item
               (-> env
                   (assoc ::query-queue rest)
                   (update ::chain-result merge-result result)
                   (as-> <>
                     (assoc <>
                       ::attributes
                       (-> <> ::chain-result ::attributes)))))))
         (gen/let [query (->> chain-result ::query distinct
                              gen/shuffle)]
           (assoc chain-result ::query (vec query))))))

   ::gen-request
   (fn [{::keys [gen-request-resolver-item max-request-attributes] :as env}]
     (gen/let [items-count (gen/choose 1 max-request-attributes)]
       (let [query (vec (take items-count base-chars))]
         (gen-request-resolver-item
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

(defn log-request-graph [req]
  (p.connector/log-entry
    (-> (runner-p3 req)
        (meta)
        :com.wsscode.pathom3.connect.runner/run-stats
        (assoc :pathom.viz.log/type :pathom.viz.log.type/plan-and-stats))))

(defn run-query-on-pathom-viz [req]
  (-> (pci/register (mapv pco/resolver (::resolvers req)))
      (p.connector/connect-env
        {:com.wsscode.pathom.viz.ws-connector.core/parser-id "debug"})
      (p.eql/process (::query req))))

(defn single-dep-prop
  [runner]
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

#_:clj-kondo/ignore

(comment
  (tap>
    (gen/sample
      ((::gen-request gen-env)
       gen-env)
      100))

  (gen/sample
    ((::gen-request gen-env)
     gen-env)
    100)

  (p.connector/log-entry
    (-> (pci/register
          [(pco/resolver 'a
             {::pco/output [:a]}
             (fn [_ _]))
           (pco/resolver 'b
             {::pco/input  [:a]
              ::pco/output [:b]}
             (fn [_ _]))

           (pco/resolver 'd
             {::pco/output [:d]}
             (fn [_ _]))

           (pco/resolver 'b2
             {::pco/input  [:d]
              ::pco/output [:b]}
             (fn [_ _]))

           (pco/resolver 'x
             {::pco/input  [:a]
              ::pco/output [:x]}
             (fn [_ _]))])
        (p.eql/process [:x :b])
        (meta)
        :com.wsscode.pathom3.connect.runner/run-stats
        (assoc :pathom.viz.log/type :pathom.viz.log.type/plan-and-stats)))
  (log-request-graph
    [])

  (doseq [req (gen/sample
                ((::gen-request gen-env)
                 (assoc gen-env
                   ::max-resolver-depth
                   10

                   ::max-deps
                   5

                   ::max-request-attributes
                   1))
                50)]
    (log-request-graph req)
    (Thread/sleep 300))

  (log-request-graph (gen/generate
                       ((::gen-request gen-env)
                        (assoc gen-env
                          ::knob-reuse-attributes?
                          false))))

  (doseq [req (gen/sample
                ((::gen-request gen-env)
                 (assoc gen-env
                   ::max-resolver-depth
                   2

                   ::max-deps
                   2

                   ::max-request-attributes
                   2))
                30)]
    (log-request-graph req)
    (Thread/sleep 300))

  (runner-p3 fail)
  (runner-p3 fail2)
  (runner-p3 fail3)

  (def fail *1)
  (def fail2 *1)
  (def fail3 *1)

  (log-request-snapshots fail)
  (log-request-snapshots fail2)
  (log-request-snapshots fail3)

  (run-query-on-pathom-viz fail)
  (run-query-on-pathom-viz fail2)

  (runner-p2 fail)

  (let [f fail2]
    {:index-oir (::pci/index-oir (pci/register (mapv pco/resolver (::resolvers f))))
     :query     (::query f)
     :expected  (::expected f)
     ;:actual    (runner-p3 f)
     })

  (check-smallest 10000 (single-dep-prop runner-p2))
  (check-smallest 10000 (single-dep-prop runner-p3))
  (check-smallest 30000
    (generate-prop
      runner-p3
      {::max-resolver-depth
       2

       ::max-deps
       1

       ::max-request-attributes
       4}))

  (check-smallest 30000
    (generate-prop
      runner-p3
      {::max-resolver-depth
       5

       ::max-deps
       3

       ::max-request-attributes
       10}))

  (check-smallest 10000 (complex-deps-prop runner-p2))
  (check-smallest 10000 (complex-deps-prop runner-p3))

  (check-smallest 1000
    (generate-prop runner-p3
      gen-env))

  (let [res (tc/quick-check 30000
              (generate-prop
                runner-p3
                {::max-resolver-depth
                 3

                 ::max-deps
                 5

                 ::max-request-attributes
                 10}))]
    (-> res
        :shrunk
        :smallest
        first))

  (let [res (tc/quick-check 30000
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

  (let [res (tc/quick-check 3000
              (generate-prop
                runner-p2
                {::max-resolver-depth
                 5

                 ::max-deps
                 8

                 ::max-request-attributes
                 10}))]
    (-> res
        :shrunk
        :smallest
        first))

  (let [res (tc/quick-check 5000
              (generate-prop
                runner-p2 {}))]
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
     gen-env)))

(comment
  (runner-p3
    {::resolvers [{::pco/op-name 'a
                   ::pco/output  [:a]
                   ::pco/resolve (fn [_ _] {:a "a"})}]
     ::query     [:a]})

  (log-request-snapshots
    {::resolvers [{::pco/op-name 'a
                   ::pco/output  [:a]
                   ::pco/resolve (fn [_ _] {:a "a"})}
                  {::pco/op-name 'a1
                   ::pco/output  [:a]
                   ::pco/resolve (fn [_ _] {:a "a"})}]
     ::query     [:a]}))
