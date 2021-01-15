(ns com.wsscode.pathom3.connect.runner.async
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [#?(:clj  com.wsscode.async.async-clj
        :cljs com.wsscode.async.async-cljs)
     :as w.async
     :refer [go-promise go-catch <? <?maybe <!maybe]]
    [com.wsscode.async.coll :as a.coll]
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
    [com.wsscode.pathom3.plugin :as p.plugin]))

(declare run-node! run-graph!)

(defn process-map-subquery
  [env ast m]
  (if (map? m)
    (let [cache-tree* (p.ent/create-entity m)
          ast         (pcr/pick-union-entry ast m)]
      (run-graph! env ast cache-tree*))
    m))

(defn process-sequence-subquery
  [env ast s]
  (go-promise
    (-> (a.coll/reduce-async
          (fn [[seq idx] entry]
            (go-promise
              [(conj seq (<?maybe (process-map-subquery (p.path/append-path env idx) ast entry)))
               (inc idx)]))
          [(empty s) 0]
          s) <?
        first)))

(defn process-map-container-subquery
  "Build a new map where the values are replaced with the map process of the subquery."
  [env ast m]
  (a.coll/reduce-kv-async
    (fn [m k v]
      (go-promise
        (assoc m k
          (<?maybe (process-map-subquery (p.path/append-path env k) ast v)))))
    (empty m)
    m))

(defn process-map-container?
  "Check if the map should be processed as a map-container, this means the sub-query
  should apply to the map values instead of the map itself.

  This can be dictated by adding the ::pcr/map-container? meta data on the value, or
  requested by the query as part of the param."
  [ast v]
  (or (-> v meta ::pcr/map-container?)
      (-> ast :params ::pcr/map-container?)))

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
        (if (process-map-container? ast v)
          (process-map-container-subquery env ast v)
          (process-map-subquery env ast v))

        (or (sequential? v)
            (set? v))
        (process-sequence-subquery env ast v)

        :else
        v)
      (if-let [x (find entity k)]
        (val x)
        v))))

(>defn merge-entity-data
  "Specialized merge versions that work on entity data."
  [env entity new-data]
  [(s/keys :opt [::pcr/merge-attribute]) ::p.ent/entity-tree ::p.ent/entity-tree
   => w.async/chan?]
  (a.coll/reduce-kv-async
    (fn [out k v]
      (if (refs/kw-identical? v ::pco/unknown-value)
        out
        (go-promise
          (let [v' (<?maybe (process-attr-subquery env entity k v))]
            (assoc out k v')))))
    entity
    new-data))

(defn merge-resolver-response!
  "This function gets the map returned from the resolver and merge the data in the
  current cache-tree."
  [env response]
  (if (map? response)
    (go-promise
      (let [new-data (<? (merge-entity-data env (p.ent/entity env) response))]
        (p.ent/reset-entity! env new-data))
      env)
    env))

(defn process-idents!
  "Process the idents from the Graph, this will add the ident data into the child.

  If there is ident data already, it gets merged with the ident value."
  [env idents]
  (go-promise
    (doseq [k idents]
      (let [sub-value (<?maybe (process-attr-subquery env {} k
                                                      (-> (get (p.ent/entity env) k)
                                                          (assoc (first k) (second k)))))]
        (p.ent/swap-entity! env #(assoc % k sub-value))))))

(defn run-next-node!
  "Runs the next node associated with the node, in case it exists."
  [{::pcp/keys [graph] :as env} {::pcp/keys [run-next]}]
  (if run-next
    (run-node! env (pcp/get-node graph run-next))))

(defn merge-node-stats!
  [{::pcr/keys [node-run-stats*]}
   {::pcp/keys [node-id]}
   data]
  (if node-run-stats*
    (swap! node-run-stats* update node-id merge data)))

(defn mark-resolver-error
  [{::pcr/keys [node-run-stats*]}
   {::pcp/keys [node-id]}
   error]
  (if node-run-stats*
    (doto node-run-stats*
      (swap! assoc-in [node-id ::pcr/node-error] error)
      (swap! update ::pcr/nodes-with-error coll/sconj node-id))))

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
  (go-promise
    (let [resolver    (pci/resolver env op-name)
          {::pco/keys [op-name batch? cache? cache-store optionals]
           :or        {cache? true}} (pco/operation-config resolver)
          env         (assoc env ::pcp/node node)
          entity      (p.ent/entity env)
          input-data  (pfsd/select-shape entity (pfsd/merge-shapes input optionals))
          input-shape (pfsd/data->shape-descriptor input-data)
          params      (pco/params env)
          start       (time/now-ms)
          cache-store (pcr/choose-cache-store env cache-store)
          result      (try
                        (if (pfsd/missing input-shape input)
                          (throw (ex-info "Insufficient data" {:required  input
                                                               :available input-shape}))
                          (cond
                            batch?
                            {::pcr/batch-hold {::pco/op-name        op-name
                                               ::pcp/node           node
                                               ::pco/cache?         cache?
                                               ::pco/cache-store    cache-store
                                               ::pcr/node-run-input input-data
                                               ::pcr/env            env}}

                            cache?
                            (p.cache/cached cache-store env
                              [op-name input-data params]
                              #(pco.prot/-resolve resolver env input-data))

                            :else
                            (pco.prot/-resolve resolver env input-data)))
                        (catch #?(:clj Throwable :cljs :default) e
                          (p.plugin/run-with-plugins env ::pcr/wrap-resolver-error
                            mark-resolver-error env node e)
                          ::pcr/node-error))
          result      (<?maybe result)
          finish      (time/now-ms)]
      (merge-node-stats! env node
                         {::pcr/resolver-run-start-ms  start
                          ::pcr/resolver-run-finish-ms finish
                          ::pcr/node-run-input         input-data
                          ::pcr/node-run-output        (if (::pcr/batch-hold result)
                                                         ::pcr/batch-hold
                                                         result)})
      result)))

(defn run-resolver-node!
  "This function evaluates the resolver associated with the node.

  First it checks if the expected results from the resolver are already available. In
  case they are, the resolver call is skipped."
  [env node]
  (if (pcr/all-requires-ready? env node)
    (run-next-node! env node)
    (go-promise
      (let [{::pcr/keys [batch-hold] :as response}
            (<?maybe (p.plugin/run-with-plugins env ::pcr/wrap-resolve
                       invoke-resolver-from-node env node))]
        (cond
          batch-hold
          (let [batch-pending (::pcr/batch-pending* env)]
            (swap! batch-pending update (::pco/op-name batch-hold) coll/vconj
              batch-hold)
            nil)

          (not (refs/kw-identical? ::pcr/node-error response))
          (do
            (<?maybe (merge-resolver-response! env response))
            (<?maybe (run-next-node! env node))))))))

(>defn run-or-node!
  [{::pcp/keys [graph]
    ::pcr/keys [choose-path]
    :or        {choose-path pcr/default-choose-path}
    :as        env} {::pcp/keys [run-or] :as or-node}]
  [(s/keys :req [::pcp/graph]) ::pcp/node => w.async/chan?]
  (go-promise
    (merge-node-stats! env or-node {::pcr/node-run-start-ms (time/now-ms)})

    (loop [nodes run-or]
      (if (seq nodes)
        (let [node-id (or (choose-path env or-node nodes)
                          (do
                            (println "Path function failed to return a path, picking first option.")
                            (first nodes)))]
          (<?maybe (run-node! env (pcp/get-node graph node-id)))
          (if (pcr/all-requires-ready? env or-node)
            (merge-node-stats! env or-node {::pcr/success-path node-id})
            (recur (disj nodes node-id))))))

    (merge-node-stats! env or-node {::pcr/node-run-finish-ms (time/now-ms)})
    (<?maybe (run-next-node! env or-node))))

(>defn run-and-node!
  "Given an AND node, runs every attached node, then runs the attached next."
  [{::pcp/keys [graph] :as env} {::pcp/keys [run-and] :as and-node}]
  [(s/keys :req [::pcp/graph]) ::pcp/node => w.async/chan?]
  (go-promise
    (merge-node-stats! env and-node {::pcr/node-run-start-ms (time/now-ms)})

    (doseq [node-id run-and]
      (<?maybe (run-node! env (pcp/get-node graph node-id))))

    (merge-node-stats! env and-node {::pcr/node-run-finish-ms (time/now-ms)})
    (<?maybe (run-next-node! env and-node))))

(>defn run-node!
  "Run a node from the compute graph. This will start the processing on the sent node
  and them will run everything that's connected to this node as sequences of it.

  The result is going to build up at ::p.ent/cache-tree*, after the run is concluded
  the output will be there."
  [env node]
  [(s/keys :req [::pcp/graph ::p.ent/entity-tree*]) ::pcp/node
   => (? w.async/chan?)]
  (case (pcp/node-kind node)
    ::pcp/node-resolver
    (run-resolver-node! env node)

    ::pcp/node-and
    (run-and-node! env node)

    ::pcp/node-or
    (run-or-node! env node)

    nil))

(defn invoke-mutation!
  "Run mutation from AST."
  [env {:keys [key params]}]
  (go-promise
    (let [mutation (pci/mutation env key)
          result   (try
                     (<?maybe (pco.prot/-mutate mutation env params))
                     (catch #?(:clj Throwable :cljs :default) e
                       {::pcr/mutation-error e}))]
      (p.ent/swap-entity! env assoc key
        (<?maybe (process-attr-subquery env {} key result))))))

(defn process-mutations!
  "Runs the mutations gathered by the planner."
  [{::pcp/keys [graph] :as env}]
  (go-promise
    (doseq [ast (::pcp/mutations graph)]
      (<?maybe (p.plugin/run-with-plugins env ::pcr/wrap-mutate
                 invoke-mutation! env ast)))))

(>defn run-graph!*
  "Run the root node of the graph. As resolvers run, the result will be add to the
  entity cache tree."
  [{::pcp/keys [graph] :as env}]
  [(s/keys :req [::pcp/graph ::p.ent/entity-tree*])
   => w.async/chan?]
  (go-promise
    (let [source-ent (p.ent/entity env)]
      ; mutations
      (<? (process-mutations! env))

      ; compute nested available fields
      (if-let [nested (::pcp/nested-available-process graph)]
        (<?maybe (merge-resolver-response! env (select-keys (p.ent/entity env) nested))))

      ; process idents
      (if-let [idents (::pcp/idents graph)]
        (<? (process-idents! env idents)))

      ; now run the nodes
      (when-let [root (pcp/get-root-node graph)]
        (<?maybe (run-node! env root)))

      ; placeholders
      (<?maybe (merge-resolver-response! env (pcr/placeholder-merge-entity env source-ent)))

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

(defn run-batches! [env]
  (let [batches* (-> env ::pcr/batch-pending*)
        batches  @batches*]
    (reset! batches* {})
    (go-promise
      (doseq [[batch-op batch-items] batches]
        (let [inputs    (mapv ::pcr/node-run-input batch-items)
              resolver  (pci/resolver env batch-op)
              batch-env (-> batch-items first ::pcr/env
                            (coll/update-if ::p.path/path #(cond-> % (seq %) pop)))
              start     (time/now-ms)
              responses (try
                          (<?maybe (pco.prot/-resolve resolver batch-env inputs))
                          (catch #?(:clj Throwable :cljs :default) e
                            (pcr/mark-batch-errors e env batch-op batch-items)))
              finish    (time/now-ms)]

          (when-not (refs/kw-identical? ::pcr/node-error responses)
            (if (not= (count inputs) (count responses))
              (throw (ex-info "Batch results must be a sequence and have the same length as the inputs." {})))

            (doseq [[{env'       ::pcr/env
                      ::pcr/keys [node-run-input]
                      ::pco/keys [cache? cache-store]
                      ::pcp/keys [node]
                      :as        batch-item} response] (map vector batch-items responses)]
              (if cache?
                (p.cache/cached cache-store env'
                  [batch-op node-run-input (pco/params batch-item)]
                  (fn [] response)))
              (merge-node-stats! env' node {::pcr/batch-run-start-ms  start
                                            ::pcr/batch-run-finish-ms finish
                                            ::pcr/node-run-output     response})
              (<?maybe (merge-resolver-response! env' response))
              (<?maybe (run-next-node! env' node))
              (if (seq (::p.path/path env'))
                (p.ent/swap-entity! env assoc-in (::p.path/path env')
                  (-> (p.ent/entity env')
                      (vary-meta assoc ::pcr/run-stats
                                 (pcr/assoc-end-plan-stats env' (::pcp/graph env')))))))))))))

(defn run-graph-impl!
  [env ast-or-graph entity-tree*]
  (go-promise
    (let [env  (pcr/setup-runner-env env entity-tree* atom)
          plan (<?maybe (plan-and-run! env ast-or-graph entity-tree*))]

      ; run batches on root path only
      (when (p.path/root? env)
        (while (seq @(::pcr/batch-pending* env))
          (<? (run-batches! env))))

      ; return result with run stats in meta
      (-> (p.ent/entity env)
          (vary-meta assoc ::pcr/run-stats (pcr/assoc-end-plan-stats env plan))))))

(>defn run-graph!
  "Plan and execute a request, given an environment (with indexes), the request AST
  and the entity-tree*."
  [env ast-or-graph entity-tree*]
  [(s/keys) (s/or :ast :edn-query-language.ast/node
                  :graph ::pcp/graph) ::p.ent/entity-tree*
   => w.async/chan?]
  (p.plugin/run-with-plugins env ::pcr/wrap-run-graph!
    run-graph-impl! env ast-or-graph entity-tree*))
