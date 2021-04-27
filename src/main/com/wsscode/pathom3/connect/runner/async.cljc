(ns com.wsscode.pathom3.connect.runner.async
  "WARN: this part of the library is under experimentation process, the design, interface
  and performance characteristics might change."
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
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
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.path :as p.path]
    [com.wsscode.pathom3.plugin :as p.plugin]
    [com.wsscode.promesa.macros :refer [clet]]
    [promesa.core :as p]))

(declare run-node! run-graph!)

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
    (let [cache-tree* (p.ent/create-entity m)
          ast         (pcr/pick-union-entry ast m)]
      (run-graph! env ast cache-tree*))
    m))

(defn process-sequence-subquery
  [env ast s]
  (if (pco/final-value? s)
    s
    (-> (reduce-async
          (fn [[seq idx] entry]
            (p/let [sub-res (process-map-subquery (p.path/append-path env idx) ast entry)]
              [(conj seq sub-res)
               (inc idx)]))
          [(empty s) 0]
          s)
        (p/then first))))

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
  [(s/keys :req [::pcp/graph]) map? ::p.path/path-entry any?
   => any?]
  (let [{:keys [children] :as ast} (pcr/entry-ast graph k)
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
        (p/let [v' (process-attr-subquery env entity k v)]
          (assoc out k v'))))
    entity
    new-data))

(defn merge-resolver-response!
  "This function gets the map returned from the resolver and merge the data in the
  current cache-tree."
  [env response]
  (if (map? response)
    (p/let [new-data (merge-entity-data env (p.ent/entity env) response)]
      (p.ent/reset-entity! env new-data)
      env)
    env))

(defn process-idents!
  "Process the idents from the Graph, this will add the ident data into the child.

  If there is ident data already, it gets merged with the ident value."
  [env idents]
  (-> (reduce-async
        (fn [_ k]
          (p/let [sub-value (process-attr-subquery env {} k
                                                   (-> (get (p.ent/entity env) k)
                                                       (assoc (first k) (second k))))]
            (p.ent/swap-entity! env #(assoc % k sub-value))))
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
      [op-name input-data params]
      #(try
         (pco.prot/-resolve resolver env input-data)
         (catch #?(:clj Throwable :cljs :default) e
           (p/rejected e))))

    (try
      (pco.prot/-resolve resolver env input-data)
      (catch #?(:clj Throwable :cljs :default) e
        (p/rejected e)))))

(defn- invoke-resolver-cached-batch
  [env cache? op-name resolver cache-store input-data params]
  (pcr/warn-batch-unsupported env op-name)
  (if cache?
    (p.cache/cached cache-store env
      [op-name input-data params]
      #(try
         (clet [res (pco.prot/-resolve resolver env [input-data])]
           (first res))
         (catch #?(:clj Throwable :cljs :default) e
           (p/rejected e))))

    (try
      (clet [res (pco.prot/-resolve resolver env [input-data])]
        (first res))
      (catch #?(:clj Throwable :cljs :default) e
        (p/rejected e)))))

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
         :or        {cache? true}} (pco/operation-config resolver)
        env             (assoc env ::pcp/node node)
        entity          (p.ent/entity env)
        input-data      (pfsd/select-shape-filtering entity (pfsd/merge-shapes input optionals) input)
        input-shape     (pfsd/data->shape-descriptor input-data)
        params          (pco/params env)
        cache-store     (pcr/choose-cache-store env cache-store)
        resolver-cache* (get env cache-store)
        _               (pcr/merge-node-stats! env node
                          {::pcr/resolver-run-start-ms (time/now-ms)})
        result          (-> (if (pfsd/missing input-shape input entity)
                              (if (pcr/missing-maybe-in-pending-batch? env input)
                                (pcr/wait-batch-response env node)
                                (p/rejected (ex-info "Insufficient data" {:required  input
                                                                          :available input-shape})))
                              (cond
                                batch?
                                (if-let [x (find @resolver-cache* [op-name input-data params])]
                                  (val x)
                                  (if (::pcr/unsupported-batch? env)
                                    (invoke-resolver-cached-batch
                                      env cache? op-name resolver cache-store input-data params)
                                    (pcr/batch-hold-token env cache? op-name node cache-store input-data)))

                                :else
                                (invoke-resolver-cached
                                  env cache? op-name resolver cache-store input-data params)))
                            (p/catch
                              (fn [error]
                                (pcr/mark-resolver-error-with-plugins env node error)
                                ::pcr/node-error)))]
    (p/let [result result]
      (let [finish (time/now-ms)]
        (pcr/merge-node-stats! env node
          (cond-> {::pcr/resolver-run-finish-ms finish}
            (not (::pcr/batch-hold result))
            (merge (pcr/report-resolver-io-stats env input-data result)))))
      result)))

(defn run-resolver-node!
  "This function evaluates the resolver associated with the node.

  First it checks if the expected results from the resolver are already available. In
  case they are, the resolver call is skipped."
  [env node]
  (if (pcr/all-requires-ready? env node)
    (run-next-node! env node)
    (p/let [_ (pcr/merge-node-stats! env node {::pcr/node-run-start-ms (time/now-ms)})
            env' (assoc env ::pcp/node node)
            {::pcr/keys [batch-hold] :as response}
            (p.plugin/run-with-plugins env' ::pcr/wrap-resolve
              invoke-resolver-from-node env' node)]
      (cond
        batch-hold response

        (not (refs/kw-identical? ::pcr/node-error response))
        (p/do!
          (merge-resolver-response! env response)
          (pcr/merge-node-stats! env node {::pcr/node-run-finish-ms (time/now-ms)})
          (run-next-node! env node))

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
   nodes]
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
            _              (run-node! env (pcp/get-node graph node-id))]
      (if (pcr/all-requires-ready? env or-node)
        (pcr/merge-node-stats! env or-node {::pcr/success-path node-id})
        (run-or-node!* env or-node (disj nodes node-id))))))

(>defn run-or-node!
  [env {::pcp/keys [run-or] :as or-node}]
  [(s/keys :req [::pcp/graph]) ::pcp/node => p/promise?]
  (p/do!
    (pcr/merge-node-stats! env or-node {::pcr/node-run-start-ms (time/now-ms)})

    (run-or-node!* env or-node run-or)

    (pcr/merge-node-stats! env or-node {::pcr/node-run-finish-ms (time/now-ms)})
    (run-next-node! env or-node)))

(>defn run-and-node!
  "Given an AND node, runs every attached node, then runs the attached next."
  [{::pcp/keys [graph] :as env} {::pcp/keys [run-and] :as and-node}]
  [(s/keys :req [::pcp/graph]) ::pcp/node => p/promise?]
  (p/do!
    (pcr/merge-node-stats! env and-node {::pcr/node-run-start-ms (time/now-ms)})

    (p/let [res
            (p/loop [xs run-and]
              (if xs
                (p/let [node-id  (first xs)
                        node-res (run-node! env (pcp/get-node graph node-id))]
                  (if (::pcr/batch-hold node-res)
                    node-res
                    (p/recur (next xs))))))]
      (if (::pcr/batch-hold res)
        res
        (do
          (pcr/merge-node-stats! env and-node {::pcr/node-run-finish-ms (time/now-ms)})
          (run-next-node! env and-node))))))

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
                           (p.plugin/run-with-plugins env ::pcr/wrap-mutate
                             #(pco.prot/-mutate mutation %1 (:params %2)) env ast)
                           (throw (ex-info "Mutation not found" {::pco/op-name key}))))
                       (p/catch (fn [e] {::pcr/mutation-error e})))
          _        (pcr/merge-mutation-stats! env {::pco/op-name key}
                     {::pcr/mutation-run-finish-ms (time/now-ms)})
          result'  (if (::pcr/mutation-error result)
                     result
                     (process-attr-subquery env {} key result))]

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

(defn run-root-node!
  [{::pcp/keys [graph] :as env}]
  (if-let [root (pcp/get-root-node graph)]
    (p/let [{::pcr/keys [batch-hold]} (run-node! env root)]
      (if batch-hold
        (if (::pcr/nested-waiting? batch-hold)
          ; add to wait
          (refs/gswap! (::pcr/batch-waiting* env) coll/vconj batch-hold)
          ; add to batch pending
          (refs/gswap! (::pcr/batch-pending* env) update (::pco/op-name batch-hold)
                       coll/vconj batch-hold))))))

(>defn run-graph!*
  "Run the root node of the graph. As resolvers run, the result will be add to the
  entity cache tree."
  [{::pcp/keys [graph] :as env}]
  [(s/keys :req [::pcp/graph ::p.ent/entity-tree*])
   => p/promise?]
  (let [source-ent (p.ent/entity env)]
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

      ; placeholders
      (merge-resolver-response! env (pcr/placeholder-merge-entity env source-ent))

      graph)))

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
                    ::pcr/compute-plan-run-finish-ms finish-plan)))]
    (run-graph!*
      (assoc env
        ::pcp/graph graph
        ::p.ent/entity-tree* entity-tree*))))

(defn run-batches-pending! [env]
  (let [batches* (-> env ::pcr/batch-pending*)
        batches  @batches*]
    (reset! batches* {})

    (reduce-async
      (fn [_ [batch-op batch-items]]
        (p/let [inputs    (mapv ::pcr/node-resolver-input batch-items)
                resolver  (pci/resolver env batch-op)
                batch-env (-> batch-items first ::pcr/env
                              (coll/update-if ::p.path/path #(cond-> % (seq %) pop)))
                start     (time/now-ms)
                responses (-> (pco.prot/-resolve resolver batch-env inputs)
                              (p/catch (fn [e] (pcr/mark-batch-errors e env batch-op batch-items))))
                finish    (time/now-ms)]

          (when-not (refs/kw-identical? ::pcr/node-error responses)
            (if (not= (count inputs) (count responses))
              (throw (ex-info "Batch results must be a sequence and have the same length as the inputs." {})))

            (reduce-async
              (fn [_ [{env'       ::pcr/env
                       ::pcp/keys [node]
                       ::pcr/keys [node-resolver-input]
                       :as        batch-item} response]]
                (pcr/cache-batch-item batch-item batch-op response)
                (pcr/merge-node-stats! env' node
                  (merge {::pcr/batch-run-start-ms  start
                          ::pcr/batch-run-finish-ms finish}
                         (pcr/report-resolver-io-stats env' node-resolver-input response)))
                (p/do!
                  (merge-resolver-response! env' response)
                  (pcr/merge-node-stats! env' node {::pcr/node-run-finish-ms (time/now-ms)})
                  (run-root-node! env')
                  (when-not (p.path/root? env')
                    (p.ent/swap-entity! env assoc-in (::p.path/path env')
                      (-> (p.ent/entity env')
                          (pcr/include-meta-stats env' (::pcp/graph env')))))))
              nil
              (map vector batch-items responses)))))
      nil
      batches)))

(defn run-batches-waiting! [env]
  (let [waits* (-> env ::pcr/batch-waiting*)
        waits  @waits*]
    (reset! waits* {})
    (reduce-async
      (fn [_ {env' ::pcr/env}]
        (p.ent/reset-entity! env' (get-in (p.ent/entity env) (::p.path/path env')))

        (p/do!
          (run-root-node! env')

          (when-not (p.path/root? env')
            (p.ent/swap-entity! env assoc-in (::p.path/path env')
              (-> (p.ent/entity env')
                  (pcr/include-meta-stats env' (::pcp/graph env')))))))
      nil
      waits)))

(defn run-batches! [env]
  (p/do!
    (run-batches-pending! env)
    (run-batches-waiting! env)))

(defn run-graph-impl!
  [env ast-or-graph entity-tree*]
  (p/let [env  (pcr/setup-runner-env env entity-tree* atom)
          plan (plan-and-run! env ast-or-graph entity-tree*)]

    ; run batches on root path only
    (when (p.path/root? env)
      (p/loop [_ (p/resolved nil)]
        (when (seq @(::pcr/batch-pending* env))
          (p/recur (run-batches! env)))))

    ; return result with run stats in meta
    (-> (p.ent/entity env)
        (pcr/include-meta-stats env plan))))

(>defn run-graph!
  "Plan and execute a request, given an environment (with indexes), the request AST
  and the entity-tree*."
  [env ast-or-graph entity-tree*]
  [(s/keys) (s/or :ast :edn-query-language.ast/node
                  :graph ::pcp/graph) ::p.ent/entity-tree*
   => p/promise?]
  (pcr/run-graph-with-plugins (assoc env ::async-runner? true) ast-or-graph entity-tree* run-graph-impl!))
