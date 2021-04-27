(ns com.wsscode.pathom3.generative-tests.execution
  (:require
    ;[clojure.test :refer [deftest is are run-tests testing]]
    [clojure.test.check :as tc]
    [clojure.test.check.generators :as gen]
    [clojure.test.check.properties :as prop]
    [com.wsscode.misc.math :as math]
    [com.wsscode.pathom.connect :as pc]
    [com.wsscode.pathom.core :as p]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    #?(:clj [com.wsscode.pathom3.connect.planner :as pcp])
    [com.wsscode.pathom3.interface.eql :as p.eql]
    #?(:clj [edn-query-language.core :as eql])))

(defn next-attr [attribute]
  (let [[base current-index]
        (or (some->
              (re-find #"(.+)(\d+)$" (name attribute))
              (->> (drop 1))
              (vec)
              (update 1 #(math/parse-long %)))
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

(defn gen-num
  ([max]
   (gen/large-integer* {:min 0 :max max}))
  ([min max]
   (gen/large-integer* {:min min :max max})))

(def base-chars
  [:a :b :c :d :e :f :g :h :i :j :k :l :m
   :n :o :p :q :r :s :t :u :v :x :y :w :z])

(comment
  (next-attr :a4214)
  (take 10 (iterate next-attr :a)))

(defn attrs->expected [attrs]
  (zipmap attrs (map name attrs)))

(defn merge-resolvers [r1 r2]
  (into r1 r2))

(defn merge-queries [q1 q2]
  (into q1 q2))

(defn merge-expected [e1 e2]
  (merge e1 e2))

(defn merge-attributes [e1 e2]
  (into e1 e2))

(defn merge-result [r1 r2]
  {::resolvers  (merge-resolvers (::resolvers r1) (::resolvers r2))
   ::query      (merge-queries (::query r1) (::query r2))
   ::expected   (merge-expected (::expected r1) (::expected r2))
   ::attributes (merge-attributes (::attributes r1) (::attributes r2))})

(def blank-result
  {::resolvers  []
   ::query      []
   ::expected   {}
   ::attributes #{}})

(defn prefix-attr [prefix attr]
  (keyword (str prefix (name attr))))

(defn suffix-attr [attr suffix]
  (keyword (str (name attr) suffix)))

(def gen-env
  {::p.attr/attribute
   :a

   ::used-attributes
   #{}

   ::knob-max-resolver-depth
   10

   ::knob-max-nesting-depth
   6

   ::knob-max-deps
   5

   ::knob-max-request-attributes
   8

   ::knob-max-resolver-outputs
   10

   ::knob-max-edge-options
   5

   ::knob-reuse-attributes?
   true

   ::gen-output-for-resolver
   (fn [{::p.attr/keys [attribute]
         ::keys        [knob-max-resolver-outputs]}]
     (gen/let [extra-outputs
               (gen-frequency
                 [[3 (gen/return 0)]
                  [2 (if (pos? knob-max-resolver-outputs)
                       (gen-num 0 knob-max-resolver-outputs))]])]
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

   ; nested resolver case
   ::gen-resolver-nested
   (fn [{::p.attr/keys [attribute]}]
     (let [attribute (suffix-attr attribute "-nest-a")
           expected  {attribute {:na "na"}}
           output    [{attribute [:na]}]]
       (gen/return
         {::resolvers  [{::pco/op-name (symbol attribute)
                         ::pco/output  output
                         ::pco/resolve (fn [_ _]
                                         {attribute {:na "na"}})}]
          ::query      output
          ::expected   expected
          ::attributes #{}})))

   ; use some attribute generated before
   ::gen-resolver-reuse
   (fn [{::keys [attributes]}]
     (if (seq attributes)
       (gen/let [attr (gen/elements attributes)]
         {::resolvers  []
          ::query      [attr]
          ::expected   {attr (name attr)}
          ::attributes #{attr}})))

   ; generate OR options
   ::gen-resolver-multi-options
   (fn [{::p.attr/keys [attribute]
         ::keys        [knob-max-edge-options gen-output-for-resolver]
         :as           env}]
     (gen/let [output       (gen-output-for-resolver env)
               option-count (gen-num 2 knob-max-edge-options)
               resolvers    (apply gen/tuple
                              (mapv
                                (fn [i]
                                  (gen/let [fail? (if (= 1 i)
                                                    (gen/return false)
                                                    gen/boolean)]
                                    {::pco/op-name (symbol (str (name attribute) "-o" i))
                                     ::pco/output  output
                                     ::pco/resolve (fn [_ _]
                                                     (if fail?
                                                       (throw (ex-info "Failed Option" {})))
                                                     (attrs->expected output))}))
                                (drop 1 (range option-count))))]
       {::resolvers  resolvers
        ::query      [attribute]
        ::expected   {attribute (name attribute)}
        ::attributes (set output)}))

   ::gen-resolver-with-deps
   (fn [{::p.attr/keys [attribute]
         ::keys        [knob-max-deps gen-dep-resolver gen-output-for-resolver]
         :as           env}]
     (gen/let [deps-count (gen-num 1 knob-max-deps)
               output     (gen-output-for-resolver env)]
       (let [next-attrs (->> (iterate next-attr attribute)
                             (drop 1)
                             (map #(keyword (str (name %) "-a")))
                             (take deps-count))]
         (gen/let [next-groups
                   (apply gen/tuple
                     (mapv
                       #(gen-dep-resolver
                          (assoc env
                            ::p.attr/attribute %))
                       next-attrs))]
           (let [actual-input (into []
                                    (comp (mapcat ::query)
                                          (distinct))
                                    next-groups)]
             (gen/let [optionals (gen-num 0 (count actual-input))]
               (let [required-count (- (count actual-input) optionals)
                     actual-input'  (if (pos? optionals)
                                      (into []
                                            (concat
                                              (take required-count actual-input)
                                              (->> (drop required-count actual-input)
                                                   (mapv pco/?))))
                                      actual-input)]
                 {::resolvers  (into
                                 [{::pco/op-name (symbol attribute)
                                   ::pco/input   actual-input'
                                   ::pco/output  output
                                   ::pco/resolve (fn [_ input]
                                                   (if-not (= input (attrs->expected actual-input))
                                                     (throw (ex-info "Bad Input"
                                                                     {:expected (attrs->expected actual-input)
                                                                      :input    input})))
                                                   (attrs->expected output))}]
                                 (mapcat ::resolvers)
                                 next-groups)
                  ::query      [attribute]
                  ::expected   {attribute (name attribute)}
                  ::attributes (reduce into (set output) (map ::attributes next-groups))})))))))

   ::gen-resolver-leaf
   (fn [{::keys [gen-resolver-no-deps
                 gen-resolver-reuse
                 knob-reuse-attributes?] :as env}]
     (gen-one-of
       [(gen-resolver-no-deps env)
        (if knob-reuse-attributes?
          (gen-resolver-reuse env))]))

   ::gen-dep-resolver
   (fn [{::keys [knob-max-resolver-depth
                 gen-resolver-leaf
                 gen-resolver-with-deps
                 gen-resolver-multi-options] :as env}]
     (let [env (update env ::knob-max-resolver-depth dec)]
       (if (pos? knob-max-resolver-depth)
         (gen-frequency
           [[3 (gen-resolver-leaf env)]
            [2 (gen-resolver-with-deps env)]
            [1 (gen-resolver-multi-options env)]])
         (gen-resolver-leaf env))))

   ::gen-resolver
   (fn [{::keys [knob-max-resolver-depth
                 gen-resolver-leaf
                 gen-resolver-with-deps
                 gen-resolver-nested
                 gen-resolver-multi-options] :as env}]
     (let [env (update env ::knob-max-resolver-depth dec)]
       (if (pos? knob-max-resolver-depth)
         (gen-frequency
           [[3 (gen-resolver-leaf env)]
            [2 (gen-resolver-with-deps env)]
            [1 (gen-resolver-nested env)]
            [1 (gen-resolver-multi-options env)]])
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
   (fn [{::keys [gen-request-resolver-item knob-max-request-attributes] :as env}]
     (gen/let [items-count (gen-num 1 knob-max-request-attributes)]
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

#?(:clj
   (defn log-request-snapshots [req]
     ((requiring-resolve 'com.wsscode.pathom.viz.ws-connector.pathom3/log-entry)
      {:pathom.viz.log/type :pathom.viz.log.type/plan-snapshots
       :pathom.viz.log/data (pcp/compute-plan-snapshots
                              (-> (pci/register (mapv pco/resolver (::resolvers req)))
                                  (assoc :edn-query-language.ast/node
                                    (eql/query->ast (::query req)))))})))

#?(:clj
   (defn log-request-graph [req]
     ((requiring-resolve 'com.wsscode.pathom.viz.ws-connector.pathom3/log-entry)
      {:pathom.viz.log/type :pathom.viz.log.type/plan-view
       :pathom.viz.log/data (-> (runner-p3 req)
                                (meta)
                                :com.wsscode.pathom3.connect.runner/run-stats)})))

#?(:clj
   (defn log-trace [req]
     ((requiring-resolve 'com.wsscode.pathom.viz.ws-connector.pathom3/log-entry)
      {:pathom.viz.log/type :pathom.viz.log.type/trace
       :pathom.viz.log/data (runner-p3 req)})))

#?(:clj
   (defn run-query-on-pathom-viz [req]
     (-> (pci/register (mapv pco/resolver (::resolvers req)))
         ((requiring-resolve 'com.wsscode.pathom.viz.ws-connector.pathom3/connect-env)
          "debug")
         (p.eql/process (::query req)))))

(defn single-dep-prop
  [runner]
  (generate-prop
    runner
    {::knob-max-resolver-depth
     5

     ::knob-max-deps
     1

     ::knob-max-request-attributes
     10}))

(defn complex-deps-prop [runner]
  (generate-prop
    runner
    {::knob-max-resolver-depth
     2

     ::knob-max-deps
     2

     ::knob-max-request-attributes
     2}))

(defn result-smallest [result]
  (-> result
      :shrunk
      :smallest
      first))

(defn check-smallest [n prop]
  (-> (tc/quick-check n prop)
      result-smallest))

#?(:clj
   (defn log-samples [n config]
     (let [sample (gen/sample
                    ((::gen-request gen-env)
                     (merge gen-env config))
                    n)]
       (doseq [req sample]
         (log-request-graph req)
         (Thread/sleep 300))

       sample)))

#?(:clj
   (defn log-trace-samples [n config]
     (let [sample (gen/sample
                    ((::gen-request gen-env)
                     (merge gen-env config))
                    n)]
       (doseq [req sample]
         (log-trace req)
         (Thread/sleep 500))

       sample)))

#_:clj-kondo/ignore

;; basic runs

(comment
  ; 10 sample
  (gen/sample
    ((::gen-request gen-env)
     gen-env)
    10)

  ; log samples
  (log-samples 10 {})
  (log-trace-samples 10 {})

  ; test all default p3
  (check-smallest 100
    (generate-prop runner-p3 {}))
  )

#_ :clj-kondo/ignore

(comment

  (def x *1)

  (nth x 4)

  (log-trace x)
  (log-request-graph x)
  (log-request-snapshots (nth x 2))
  (run-query-on-pathom-viz (nth x 2))
  (log-request-snapshots (nth x 4))
  (meta (runner-p3 x))

  (tap>
    (gen/sample
      ((::gen-request gen-env)
       gen-env)
      10))

  (log-request-graph
    [])

  (doseq [req (gen/sample
                ((::gen-request gen-env)
                 gen-env)
                10)]
    (log-request-graph req)
    (Thread/sleep 300))

  (log-request-graph (gen/generate
                       ((::gen-request gen-env)
                        (assoc gen-env
                          ::knob-reuse-attributes?
                          false))))

  (log-samples 5 {})

  (log-samples 20
    {::knob-max-resolver-depth
     6

     ::knob-max-deps
     3

     ::knob-max-request-attributes
     2

     ::knob-max-resolver-outputs
     10})

  (log-request-snapshots (last sample))
  (log-request-snapshots (second (reverse sample)))

  (meta (runner-p3 (second (reverse sample))))

  (runner-p3 fail)
  (runner-p3 fail2)
  (runner-p3 fail3)

  (def fail *1)
  (def fail2 *1)
  (def fail3 (second (reverse sample)))

  (log-request-snapshots fail)
  (log-request-snapshots fail2)
  (log-request-snapshots fail3)

  (run-query-on-pathom-viz fail)
  (run-query-on-pathom-viz fail2)

  (runner-p2 fail)

  (let [f (second (reverse *2))]
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
      {::knob-max-resolver-depth
       2

       ::knob-max-deps
       1

       ::knob-max-request-attributes
       4}))

  (check-smallest 30000
    (generate-prop
      runner-p3
      {::knob-max-resolver-depth
       5

       ::knob-max-deps
       3

       ::knob-max-request-attributes
       10}))

  (check-smallest 10000 (complex-deps-prop runner-p2))
  (check-smallest 10000 (complex-deps-prop runner-p3))


  (let [res (tc/quick-check 30000
              (generate-prop
                runner-p3
                {::knob-max-resolver-depth
                 3

                 ::knob-max-deps
                 5

                 ::knob-max-request-attributes
                 10}))]
    (-> res
        :shrunk
        :smallest
        first))

  (let [res (tc/quick-check 30000
              (generate-prop
                runner-p3
                {::knob-max-resolver-depth
                 2

                 ::knob-max-deps
                 2

                 ::knob-max-request-attributes
                 2}))]
    (-> res
        :shrunk
        :smallest
        first))

  (let [res (tc/quick-check 3000
              (generate-prop
                runner-p2
                {::knob-max-resolver-depth
                 5

                 ::knob-max-deps
                 8

                 ::knob-max-request-attributes
                 10}))]
    (-> res
        :shrunk
        :smallest
        first))


  (check-smallest 500
    (generate-prop runner-p3
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
                   ::pco/resolve (fn [_ _] {:a "a"})}]
     ::query     [:a]})

  (runner-p3
    {::resolvers [{::pco/op-name 'a
                   ::pco/input   [:b]
                   ::pco/output  [:a]
                   ::pco/resolve (fn [_ _] {:a "a"})}
                  {::pco/op-name 'b
                   ::pco/output  [:b]
                   ::pco/resolve (fn [_ _] {:b "a"})}]
     ::query     [:a :b]})

  (runner-p3
    {::resolvers [{::pco/op-name 'a
                   ::pco/input   [:b]
                   ::pco/output  [:a]
                   ::pco/resolve (fn [_ _] {:a "a"})}
                  {::pco/op-name 'b
                   ::pco/input   [:c]
                   ::pco/output  [:b]
                   ::pco/resolve (fn [_ _] {:b "b"})}
                  {::pco/op-name 'c
                   ::pco/output  [:c]
                   ::pco/resolve (fn [_ _] {:c "c"})}]
     ::query     [:a]})

  (meta (runner-p3 fail))
  (log-request-graph fail)

  (runner-p3
    {::resolvers [{::pco/op-name 'a
                   ::pco/input   [(pco/? :b)]
                   ::pco/output  [:a]
                   ::pco/resolve (fn [_ {:keys [b]}] {:a (str "a" b)})}
                  ]
     ::query     [:a]})

  (log-request-snapshots
    {::resolvers [{::pco/op-name 'a
                   ::pco/input   [:b]
                   ::pco/output  [:a]
                   ::pco/resolve (fn [_ _] {:a "a"})}
                  {::pco/op-name 'b
                   ::pco/input   [:c]
                   ::pco/output  [:b]
                   ::pco/resolve (fn [_ _] {:b "b"})}
                  {::pco/op-name 'c
                   ::pco/output  [:c]
                   ::pco/resolve (fn [_ _] {:c "c"})}]
     ::query     [:a]})

  (log-request-snapshots
    {::resolvers [(pco/resolver 'users
                    {::pco/output [{:users [:user/id]}]}
                    (fn [_ _]
                      {:users [{:user/id 1}
                               {:user/id 2}]}))
                  (pbir/static-attribute-map-resolver :user/id :user/score
                    {1 10
                     2 20})
                  (pco/resolver 'total-score
                    {::pco/input  [{:users [(pco/? :user/score)]}]
                     ::pco/output [:total-score]}
                    (fn [_ {:keys [users]}]
                      {:total-score (reduce + 0 (map :user/score users))}))]
     ::query     [:total-score]})

  (log-request-snapshots
    {::resolvers [{::pco/op-name 'a
                   ::pco/input   [:b]
                   ::pco/output  [:a]
                   ::pco/resolve (fn [_ _] {:a "a"})}
                  {::pco/op-name 'cb
                   ::pco/input   [:c]
                   ::pco/output  [:b]
                   ::pco/resolve (fn [_ _] {:b "b"})}
                  {::pco/op-name 'db
                   ::pco/input   [:d]
                   ::pco/output  [:b]
                   ::pco/resolve (fn [_ _] {:b "b"})}
                  {::pco/op-name 'c
                   ::pco/output  [:c]
                   ::pco/resolve (fn [_ _] {:c "c"})}
                  {::pco/op-name 'd
                   ::pco/output  [:d]
                   ::pco/resolve (fn [_ _] {:d "d"})}]
     ::query     [:a]}))
