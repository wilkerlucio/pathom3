(ns com.wsscode.pathom3.connect.runner.parallel
  "WARN: this part of the library is under experimentation process, the design, interface
  and performance characteristics might change."
  (:require
    [clojure.core.async :as async :refer [<! go-loop put!]]
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [com.fulcrologic.guardrails.core :refer [=> >def >defn ?]]
    [com.wsscode.log :as l]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.misc.refs :as refs]
    [com.wsscode.misc.time :as time]
    [com.wsscode.pathom3.cache :as p.cache]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.operation.protocols :as pco.prot]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.connect.runner.async :as pcra]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.path :as p.path]
    [com.wsscode.pathom3.plugin :as p.plugin]
    [com.wsscode.promesa.macros :refer [ctry]]
    [promesa.core :as p]))

(>def ::env (s/or :env (s/keys) :env-promise p/promise?))
(>def ::batch-hold-delay-ms nat-int?)
(>def ::batch-hold-flush-threshold nat-int?)

(declare run-node! run-graph!
         run-graph-entity-done
         run-root-node!)

(defn- reduce-async [f init coll]
  (reduce
    (fn [d item]
      (p/then d #(f % item)))
    (p/resolved init)
    coll))

(defn- reduce-kv-async [f init coll]
  (reduce-kv
    (fn [d k v]
      (p/then d #(f % k v)))
    (p/resolved init)
    coll))

(defn process-map-subquery
  [env ast m]
  (if (and (map? m)
           (not (pco/final-value? m)))
    (if-let [[ast cache-tree*] (pcr/process-map-subquery-data ast m)]
      (run-graph! env ast cache-tree*))
    m))

(defn process-sequence-subquery
  [env ast s]
  (if (pco/final-value? s)
    s
    (p/let [results
            (p/all
              (into []
                    (map-indexed
                      (fn [idx entry]
                        (p.plugin/run-with-plugins env
                          ::pcr/wrap-process-sequence-item
                          process-map-subquery
                          (p.path/append-path env idx)
                          ast
                          entry)))
                    s))]
      (cond-> (into (empty s) (filter some?) results)
        (coll/coll-append-at-head? s)
        reverse))))

(defn process-map-container-subquery
  "Build a new map where the values are replaced with the map process of the subquery."
  [env ast m]
  (if (pco/final-value? m)
    m
    (reduce-kv-async
      (fn [m k v]
        (p/let [res (process-map-subquery (p.path/append-path env k) ast v)]
          (assoc m k res)))
      (empty m)
      m)))

(>defn process-attr-subquery
  [{::pcp/keys [graph]
    :as        env} entity k v]
  [(s/keys :req [::pcp/graph]) map?
   (s/or :path ::p.path/path-entry
         :ast :edn-query-language.ast/node) any?
   => any?]
  (let [{:keys [children] :as ast} (pcr/process-attr-subquery-ast graph k)
        k   (pcr/process-attr-subquery-key k)
        env (p.path/append-path env k)]
    (if children
      (cond
        (map? v)
        (if (pcr/process-map-container? ast v)
          (process-map-container-subquery env ast v)
          (process-map-subquery env ast v))

        (coll/collection? v)
        (process-sequence-subquery
          (cond-> env
            ; no batch in sequences that are not vectors because we can't reach those
            ; paths for updating later
            (not (vector? v))
            (assoc ::pcr/unsupported-batch? true))
          ast v)

        :else
        v)
      (if-let [x (find entity k)]
        (val x)
        v))))

(>defn merge-entity-data
  "Specialized merge versions that work on entity data."
  [env entity new-data]
  [(s/keys :opt [::pcr/merge-attribute]) ::p.ent/entity-tree ::p.ent/entity-tree
   => p/promise?]
  (reduce-kv-async
    (fn [out k v]
      (if (refs/kw-identical? v ::pco/unknown-value)
        out
        (p.plugin/run-with-plugins env ::pcr/wrap-merge-attribute
          (fn merge-entity-data--internal [env m k v]
            (p/let [v' (process-attr-subquery env entity k v)]
              (assoc m k v')))
          env out k v)))
    entity
    new-data))

(defn merge-resolver-response!
  "This function gets the map returned from the resolver and merge the data in the
  current cache-tree."
  [env response]
  (if (map? response)
    (p/let [new-data (merge-entity-data env (p.ent/entity env) response)]
      (p.ent/swap-entity! env coll/deep-merge new-data)
      env)
    env))

(defn process-idents!
  "Process the idents from the Graph, this will add the ident data into the child.

  If there is ident data already, it gets merged with the ident value."
  [env idents]
  (-> (reduce-async
        (fn [_ k]
          (p/let [entity  (p.ent/entity env)
                  entity' (p.plugin/run-with-plugins env ::pcr/wrap-merge-attribute
                            (fn process-idents-merge-attr--internal [env m k v]
                              (p/let [sub-value (process-attr-subquery env entity k v)]
                                (assoc m k sub-value)))
                            env {} k (assoc (get entity k) (first k) (second k)))]
            (p.ent/swap-entity! env #(assoc % k (get entity' k)))))
        nil
        idents)
      (p/then (constantly nil))))

(defn run-next-node!
  "Runs the next node associated with the node, in case it exists."
  [{::pcp/keys [graph] :as env} {::pcp/keys [run-next]}]
  (if run-next
    (run-node! env (pcp/get-node graph run-next))))

(defn- invoke-resolver-cached
  [env cache? op-name resolver cache-store input-data params]
  (if cache?
    (p.cache/cached cache-store env
      (pcr/cache-key env input-data op-name params)
      #(try
         (pcr/invoke-resolver-with-plugins resolver env input-data)
         (catch #?(:clj Throwable :cljs :default) e
           (p/rejected e))))

    (try
      (pcr/invoke-resolver-with-plugins resolver env input-data)
      (catch #?(:clj Throwable :cljs :default) e
        (p/rejected e)))))

(>defn delayed-process
  ([f debounce-window-ms]
   [fn? ::batch-hold-delay-ms => any?]
   (delayed-process f debounce-window-ms nil))
  ([f debounce-window-ms flush-threshold]
   [fn? ::batch-hold-delay-ms (? ::batch-hold-flush-threshold) => any?]
   (let [income (async/chan 1024)]
     (go-loop [buffer (list)]
       (if (seq buffer)
         (if (and flush-threshold (>= (count buffer) flush-threshold))
           ; threshold flush
           (do
             (f (reverse buffer))
             (recur (list)))
           ; timer flush
           (let [timer (async/timeout debounce-window-ms)
                 [val port] (async/alts! [income timer] :priority true)]
             (if (= port timer)
               (do
                 (f (reverse buffer))
                 (recur (list)))
               (recur (conj buffer val)))))
         (when-let [item (<! income)]
           ; wait for input
           (recur (conj buffer item)))))
     income)))

(comment
  (def x (delayed-process clojure.pprint/pprint 100))
  (def c (atom 0))
  (dotimes [_ 10]
    (put! x (swap! c inc))
    @(p/delay (rand-int 200))))

(defn run-async-batches! [env batch-items]
  (p/let [batch-op     (-> batch-items first ::pco/op-name)
          resolver     (pci/resolver env batch-op)
          input-groups (pcr/batch-group-input-groups batch-items)
          inputs       (pcr/batch-group-inputs batch-items)
          batch-env    (-> batch-items first ::pcr/env
                           (coll/update-if ::p.path/path #(cond-> % (seq %) pop)))
          max-size     (-> resolver pco/operation-config ::pco/batch-chunk-size)
          start        (time/now-ms)
          responses    (-> (p/do!
                             (pcra/invoke-async-maybe-split-batches max-size resolver batch-env batch-op input-groups inputs))
                           (p/catch (fn [e]
                                      (try
                                        (pcr/mark-batch-errors e env batch-op batch-items)
                                        (catch #?(:clj Throwable :cljs :default) _ nil))
                                      {::pcr/node-error e})))
          finish       (time/now-ms)]

    (if-let [err (::pcr/node-error responses)]
      (doseq [{::keys [batch-response-promise]}
              (into [] cat (vals input-groups))]
        (p/reject! batch-response-promise err))

      (do
        (if (not= (count inputs) (count responses))
          (throw (ex-info "Batch results must be a sequence and have the same length as the inputs." {})))

        (doseq [[{env'       ::pcr/env
                  ::keys     [batch-response-promise]
                  ::pcp/keys [node]
                  ::pcr/keys [node-resolver-input]
                  :as        batch-item} response]
                (pcr/combine-inputs-with-responses input-groups inputs responses)]
          (pcr/cache-batch-item batch-item batch-op response)

          (pcr/merge-node-stats! env' node
            (merge {::pcr/batch-run-start-ms  start
                    ::pcr/batch-run-finish-ms finish}
                   (pcr/report-resolver-io-stats env' node-resolver-input response)))

          (p/resolve! batch-response-promise response))))))

(defn create-batch-processor
  [{::keys [batch-hold-delay-ms
            batch-hold-flush-threshold]
    :as    env}]
  (delayed-process #(run-async-batches! env %)
                   batch-hold-delay-ms
                   batch-hold-flush-threshold))

(defn get-batch-process
  [{::keys [async-batches*] :as env} batch-split]
  (-> (swap! async-batches*
        (fn [batches]
          (if (contains? batches batch-split)
            batches
            (assoc batches batch-split
              (create-batch-processor env)))))
      (get batch-split)))

(defn invoke-async-batch
  [env cache? op-name node cache-store input-data params]
  (let [process  (get-batch-process env {::pco/op-name op-name
                                         ::pcp/params  params})
        response (p/deferred)]
    (put! process
          (-> (pcr/batch-hold-token env cache? op-name node cache-store input-data params)
              ::pcr/batch-hold
              (assoc ::batch-response-promise response)))
    response))

(defn invoke-resolver-from-node
  "Evaluates a resolver using node information.

  When this function runs the resolver, if filters the data to only include the keys
  mentioned by the resolver input. This is important to ensure that the resolver is
  not using some key that came accidentally due to execution order, that would lead to
  brittle executions."
  [env
   {::pco/keys [op-name]
    ::pcp/keys [input]
    :as        node}]
  (let [resolver        (pci/resolver env op-name)
        {::pco/keys [op-name batch? cache? cache-store optionals]
         :or        {cache? true}
         :as        r-config} (pco/operation-config resolver)
        env             (assoc env ::pcp/node node)
        entity          (p.ent/entity env)
        input-data      (pfsd/select-shape-filtering entity (pfsd/merge-shapes input optionals) input)
        input-data      (pcr/enhance-dynamic-input r-config node input-data)
        params          (pco/params env)
        cache-store     (pcr/choose-cache-store env cache-store)
        resolver-cache* (get env cache-store)]
    (pcr/merge-node-stats! env node
      {::pcr/resolver-run-start-ms (time/now-ms)})
    (p/let [response (-> (if-let [missing (pfsd/missing-from-data entity input)]
                           (pcr/report-resolver-error
                             (assoc env :com.wsscode.pathom3.error/lenient-mode? true)
                             node
                             (ex-info (str "Insufficient data calling resolver '" op-name ". Missing attrs " (str/join "," (keys missing)))
                                      {:required  input
                                       :available (pfsd/data->shape-descriptor input-data)
                                       :missing   missing}))
                           (cond
                             batch?
                             (if-let [x (p.cache/cache-find resolver-cache* [op-name input-data params])]
                               (val x)
                               (invoke-async-batch
                                 env cache? op-name node cache-store input-data params))

                             :else
                             (invoke-resolver-cached
                               env cache? op-name resolver cache-store input-data params)))
                         (p/catch
                           (fn [error]
                             (pcr/report-resolver-error env node error))))
            response (pcr/validate-response! env node response)]
      (let [finish (time/now-ms)]
        (pcr/merge-node-stats! env node
          (-> {::pcr/resolver-run-finish-ms finish}
              (merge (pcr/report-resolver-io-stats env input-data response)))))
      response)))

(defn run-resolver-node!
  "This function evaluates the resolver associated with the node.

  First it checks if the expected results from the resolver are already available. In
  case they are, the resolver call is skipped."
  [env node]
  (if (or (pcr/resolver-already-ran? env node) (pcr/all-requires-ready? env node))
    (run-next-node! env node)
    (p/let [_        (pcr/merge-node-stats! env node {::pcr/node-run-start-ms (time/now-ms)})
            env'     (assoc env ::pcp/node node)
            response (invoke-resolver-from-node env' node)]
      (cond
        (or (not (refs/kw-identical? ::pcr/node-error response))
            (pcp/node-optional? node))
        (p/do!
          (merge-resolver-response! env response)
          (pcr/merge-node-stats! env node {::pcr/node-run-finish-ms (time/now-ms)})
          (if-not (and (::pcp/node-resolution-checkpoint? node)
                       (pcr/user-demand-completed? env))
            (run-next-node! env node)))

        :else
        (do
          (pcr/merge-node-stats! env node {::pcr/node-run-finish-ms (time/now-ms)})
          nil)))))

(defn run-or-node!*
  [{::pcp/keys [graph]
    ::pcr/keys [choose-path]
    :or        {choose-path pcr/default-choose-path}
    :as        env}
   or-node
   nodes
   errors]
  (if (seq nodes)
    (p/let [picked-node-id (choose-path env or-node nodes)
            node-id        (if (contains? nodes picked-node-id)
                             picked-node-id
                             (do
                               (l/warn ::pcr/event-invalid-chosen-path
                                       {:expected-one-of nodes
                                        :chosen-attempt  picked-node-id
                                        :actual-used     (first nodes)})
                               (first nodes)))
            _              (pcr/add-taken-path! env or-node node-id)
            node-res       (-> (p/let [res (run-node! env (pcp/get-node graph node-id))]
                                 res)
                               (p/catch #(array-map ::pcr/or-option-error %)))]
      (cond
        (::pcr/batch-hold node-res)
        node-res

        (::pcr/or-option-error node-res)
        (run-or-node!* env or-node (disj nodes node-id) (conj errors (::pcr/or-option-error node-res)))

        :else
        (if (pcr/all-requires-ready? env or-node)
          (pcr/merge-node-stats! env or-node {::pcr/success-path node-id})
          (run-or-node!* env or-node (disj nodes node-id) errors))))
    {::pcr/or-option-error errors}))

(>defn run-or-node!
  [env {::pcp/keys [run-or] :as or-node}]
  [(s/keys :req [::pcp/graph]) ::pcp/node => p/promise?]
  (p/do!
    (pcr/merge-node-stats! env or-node {::pcr/node-run-start-ms (time/now-ms)})

    (p/let [res (if-not (pcr/all-requires-ready? env or-node)
                  (run-or-node!* env or-node run-or []))]
      (cond
        (::pcr/batch-hold res)
        res

        (and (::pcr/or-option-error res)
             (not (pcr/or-expected-optional? env or-node)))
        (pcr/handle-or-error env or-node res)

        :else
        (do
          (pcr/merge-node-stats! env or-node {::pcr/node-run-finish-ms (time/now-ms)})
          (run-next-node! env or-node))))))

(>defn run-and-node!
  "Given an AND node, runs every attached node, then runs the attached next."
  [{::pcp/keys [graph] :as env} {::pcp/keys [run-and] :as and-node}]
  [(s/keys :req [::pcp/graph]) ::pcp/node => p/promise?]
  (p/do!
    (pcr/merge-node-stats! env and-node {::pcr/node-run-start-ms (time/now-ms)})

    (p/all (mapv #(run-node! env (pcp/get-node graph %)) run-and))
    (pcr/merge-node-stats! env and-node {::pcr/node-run-finish-ms (time/now-ms)})
    (run-next-node! env and-node)))

(>defn run-node!
  "Run a node from the compute graph. This will start the processing on the sent node
  and them will run everything that's connected to this node as sequences of it.

  The result is going to build up at ::p.ent/cache-tree*, after the run is concluded
  the output will be there."
  [env node]
  [(s/keys :req [::pcp/graph ::p.ent/entity-tree*]) ::pcp/node
   => (? p/promise?)]
  (case (pcp/node-kind node)
    ::pcp/node-resolver
    (run-resolver-node! env node)

    ::pcp/node-and
    (run-and-node! env node)

    ::pcp/node-or
    (run-or-node! env node)))

(defn run-foreign-mutation
  [env {:keys [key] :as ast}]
  (let [ast      (cond-> ast (not (:children ast)) (dissoc :children))
        mutation (pci/mutation env key)
        {::pco/keys [dynamic-name]} (pco/operation-config mutation)
        foreign  (pci/resolver env dynamic-name)
        {::pco/keys [batch?]} (pco/operation-config foreign)
        ast      (pcp/promote-foreign-ast-children ast)]
    (p/let [res (pco.prot/-resolve
                  foreign
                  env
                  (cond-> {::pcp/foreign-ast {:type :root :children [ast]}}
                    batch? vector))]
      (get res key))))

(defn invoke-mutation!
  "Run mutation from AST."
  [env {:keys [key] :as ast}]
  (p/let [mutation (pci/mutation env key)
          start    (time/now-ms)
          _        (pcr/merge-mutation-stats! env {::pco/op-name key}
                     {::pcr/node-run-start-ms     start
                      ::pcr/mutation-run-start-ms start})
          result   (-> (p/do!
                         (if mutation
                           (if (-> mutation pco/operation-config ::pco/dynamic-name)
                             (run-foreign-mutation env ast)
                             (p.plugin/run-with-plugins env ::pcr/wrap-mutate
                               #(pco.prot/-mutate mutation %1 (:params %2)) env ast))
                           (throw (ex-info (str "Mutation " key " not found") {::pco/op-name key}))))
                       (p/catch
                         (fn [e]
                           {::pcr/mutation-error
                            (p.plugin/run-with-plugins env ::pcr/wrap-mutation-error
                              (fn [env _ast e]
                                (pcr/fail-fast env e) e) env ast e)})))
          _        (pcr/merge-mutation-stats! env {::pco/op-name key}
                     {::pcr/mutation-run-finish-ms (time/now-ms)})
          result'  (if (::pcr/mutation-error result)
                     result
                     (process-attr-subquery env {} ast result))]

    (p.ent/swap-entity! env assoc key result')

    (pcr/merge-mutation-stats! env {::pco/op-name key}
      {::pcr/node-run-finish-ms (time/now-ms)})

    result))

(defn process-mutations!
  "Runs the mutations gathered by the planner."
  [{::pcp/keys [graph] :as env}]
  (reduce-async
    (fn [_ ast]
      (invoke-mutation! env ast))
    nil
    (::pcp/mutations graph)))

(defn run-graph-entity-done [env]
  (p/do!
    ; placeholders
    (if (-> env ::pcp/graph ::pcp/placeholders)
      (merge-resolver-response! env (pcr/placeholder-merge-entity env)))
    ; entity ready
    (p.plugin/run-with-plugins env ::pcr/wrap-entity-ready! pcr/run-graph-done! env)))

(defn run-root-node!
  [{::pcp/keys [graph] :as env}]
  (if-let [root (pcp/get-root-node graph)]
    (p/do!
      (run-node! env root)
      (run-graph-entity-done env))
    (run-graph-entity-done env)))

(>defn run-graph!*
  "Run the root node of the graph. As resolvers run, the result will be add to the
  entity cache tree."
  [{::pcp/keys [graph] :as env}]
  [(s/keys :req [::pcp/graph ::p.ent/entity-tree*])
   => p/promise?]
  (let [env (assoc env ::pcr/source-entity (p.ent/entity env))]
    (p/do!
      ; mutations
      (process-mutations! env)

      ; compute nested available fields
      (if-let [nested (::pcp/nested-process graph)]
        (merge-resolver-response! env (select-keys (p.ent/entity env) nested)))

      ; process idents
      (if-let [idents (::pcp/idents graph)]
        (process-idents! env idents))

      ; now run the nodes
      (run-root-node! env)

      env)))

(defn plan-and-run!
  [env ast-or-graph entity-tree*]
  #_; keep commented for description, but don't want to validate this fn on runtime
      [(s/keys) (s/or :ast :edn-query-language.ast/node
                      :graph ::pcp/graph) ::p.ent/entity-tree*
       => (s/keys)]
  (let [graph (if (::pcp/nodes ast-or-graph)
                ast-or-graph
                (let [start-plan  (time/now-ms)
                      plan        (pcp/compute-run-graph
                                    (assoc env
                                      :edn-query-language.ast/node ast-or-graph
                                      ::pcp/available-data (pfsd/data->shape-descriptor @entity-tree*)))
                      finish-plan (time/now-ms)]
                  (assoc plan
                    ::pcr/compute-plan-run-start-ms start-plan
                    ::pcr/compute-plan-run-finish-ms finish-plan)))
        env   (assoc env
                ::pcp/graph graph
                ::p.ent/entity-tree* entity-tree*)]
    (ctry
      (if (pcr/runnable-graph? graph)
        (run-graph!* env)
        (do
          (run-graph-entity-done env)
          env))
      (catch #?(:clj Throwable :cljs :default) e
        (throw (pcr/processor-exception env e))))))

(defn run-graph-impl!
  [env ast-or-graph entity-tree*]
  (p/let [env (-> (pcr/setup-runner-env env entity-tree* atom)
                  (coll/merge-defaults {::batch-hold-delay-ms 5
                                        ::async-batches*      (atom {})}))
          env (plan-and-run! env ast-or-graph entity-tree*)]

    ; return result with run stats in meta
    (-> (p.ent/entity env)
        (pcr/include-meta-stats env))))

(>defn run-graph!
  "Plan and execute a request, given an environment (with indexes), the request AST
  and the entity-tree*."
  [env ast-or-graph entity-tree*]
  [::env
   (s/or :ast :edn-query-language.ast/node
         :graph ::pcp/graph) ::p.ent/entity-tree*
   => p/promise?]
  (p/let [env env]
    (pcr/run-graph-with-plugins
      (assoc env :com.wsscode.pathom3.connect.runner.async/async-runner? true)
      ast-or-graph
      entity-tree* run-graph-impl!)))
