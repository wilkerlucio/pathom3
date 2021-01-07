(ns com.wsscode.pathom3.connect.runner
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.misc.refs :as refs]
    [com.wsscode.misc.time :as time]
    [com.wsscode.pathom3.cache :as p.cache]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.operation.protocols :as pco.prot]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.path :as p.path]
    [com.wsscode.pathom3.plugin :as p.plugin]))

(>def ::map-container? boolean?)
(>def ::merge-attribute fn?)
(>def ::node-error any?)

(>defn all-requires-ready?
  "Check if all requirements from the node are present in the current entity."
  [env {::pcp/keys [expects]}]
  [map? (s/keys :req [::pcp/expects])
   => boolean?]
  (let [entity (p.ent/entity env)]
    (every? #(contains? entity %) (keys expects))))

(declare run-node! run-graph!)

(defn union-key-on-data? [{:keys [union-key]} m]
  (contains? m union-key))

(defn pick-union-entry
  "Check if ast children is a union type. If so, makes a decision to choose a path and
  return that AST."
  [ast m]
  (if (pf.eql/union-children? ast)
    (some (fn [ast']
            (if (union-key-on-data? ast' m)
              (pf.eql/union->root ast')))
      (pf.eql/union-children ast))
    ast))

(defn process-map-subquery
  [env ast m]
  (if (map? m)
    (let [cache-tree* (p.ent/create-entity m)
          ast         (pick-union-entry ast m)]
      (run-graph! env ast cache-tree*))
    m))

(defn process-sequence-subquery
  [env ast s]
  (into
    (empty s)
    (map-indexed #(process-map-subquery (p.path/append-path env %) ast %2))
    s))

(defn process-map-container-subquery
  "Build a new map where the values are replaced with the map process of the subquery."
  [env ast m]
  (into {}
        (map (fn [x]
               (coll/make-map-entry
                 (key x)
                 (process-map-subquery (p.path/append-path env (key x)) ast (val x)))))
        m))

(defn process-map-container?
  "Check if the map should be processed as a map-container, this means the sub-query
  should apply to the map values instead of the map itself.

  This can be dictated by adding the ::pcr/map-container? meta data on the value, or
  requested by the query as part of the param."
  [ast v]
  (or (-> v meta ::map-container?)
      (-> ast :params ::map-container?)))

(>defn process-attr-subquery
  [{::pcp/keys [graph]
    :as        env} entity k v]
  [(s/keys :opt [:edn-query-language.ast/node]) map? ::p.path/path-entry any?
   => any?]
  (let [ast (pcp/entry-ast graph k)
        env (p.path/append-path env k)]
    (if (:children ast)
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
  [(s/keys :opt [::merge-attribute]) ::p.ent/entity-tree ::p.ent/entity-tree
   => ::p.ent/entity-tree]
  (reduce-kv
    (fn [out k v]
      (if (refs/kw-identical? v ::pco/unknown-value)
        out
        (p.plugin/run-with-plugins env ::wrap-merge-attribute
          (fn [env m k v] (assoc m k (process-attr-subquery env entity k v)))
          env out k v)))
    entity
    new-data))

(defn merge-resolver-response!
  "This function gets the map returned from the resolver and merge the data in the
  current cache-tree."
  [env response]
  (if (map? response)
    (p.ent/swap-entity! env #(merge-entity-data env % %2) response))
  env)

(defn process-idents!
  "Process the idents from the Graph, this will add the ident data into the child.

  If there is ident data already, it gets merged with the ident value."
  [env idents]
  (doseq [k idents]
    (p.ent/swap-entity! env
      #(assoc % k (process-attr-subquery env {} k
                                         (assoc (get % k) (first k) (second k)))))))

(defn run-next-node!
  "Runs the next node associated with the node, in case it exists."
  [{::pcp/keys [graph] :as env} {::pcp/keys [run-next]}]
  (if run-next
    (run-node! env (pcp/get-node graph run-next))))

(defn merge-node-stats!
  [{::keys [node-run-stats*]}
   {::pcp/keys [node-id]}
   data]
  (if node-run-stats*
    (vswap! node-run-stats* update node-id merge data)))

(defn mark-resolver-error
  [{::keys [node-run-stats*]}
   {::pcp/keys [node-id]}
   error]
  (if node-run-stats*
    (doto node-run-stats*
      (vswap! assoc-in [node-id ::node-error] error)
      (vswap! update ::nodes-with-error coll/sconj node-id))))

(defn choose-cache-store [env cache-store]
  (if cache-store
    (if (contains? env cache-store)
      cache-store
      (do
        (println "WARN: Tried to use an undefined cache store" cache-store ". Falling back to default resolver-cache*.")
        ::resolver-cache*))
    ::resolver-cache*))

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
  (let [resolver    (pci/resolver env op-name)
        {::pco/keys [op-name batch? cache? cache-store]
         :or        {cache? true}} (pco/operation-config resolver)
        env         (assoc env ::pcp/node node)
        entity      (p.ent/entity env)
        input-data  (pfsd/select-shape entity input)
        input-shape (pfsd/data->shape-descriptor input-data)
        params      (pco/params env)
        start       (time/now-ms)
        cache-store (choose-cache-store env cache-store)
        result      (try
                      (if (pfsd/missing input-shape input)
                        (throw (ex-info "Insufficient data" {:required  input
                                                             :available input-shape}))
                        (cond
                          batch?
                          {::batch-hold {::pco/op-name     op-name
                                         ::pcp/node        node
                                         ::pco/cache?      cache?
                                         ::pco/cache-store cache-store
                                         ::node-run-input  input-data
                                         ::env             env}}

                          cache?
                          (p.cache/cached cache-store env
                            [op-name input-data params]
                            #(pco.prot/-resolve resolver env input-data))

                          :else
                          (pco.prot/-resolve resolver env input-data)))
                      (catch #?(:clj Throwable :cljs :default) e
                        (p.plugin/run-with-plugins env ::wrap-resolver-error
                          mark-resolver-error env node e)
                        ::node-error))
        finish      (time/now-ms)]
    (merge-node-stats! env node
                       {::resolver-run-start-ms  start
                        ::resolver-run-finish-ms finish
                        ::node-run-input         input-data
                        ::node-run-output        (if (::batch-hold result)
                                                   ::batch-hold
                                                   result)})
    result))

(defn run-resolver-node!
  "This function evaluates the resolver associated with the node.

  First it checks if the expected results from the resolver are already available. In
  case they are, the resolver call is skipped."
  [env node]
  (if (all-requires-ready? env node)
    (run-next-node! env node)
    (let [{::keys [batch-hold] :as response}
          (p.plugin/run-with-plugins env ::wrap-resolve
            invoke-resolver-from-node env node)]
      (cond
        batch-hold
        (let [batch-pending (::batch-pending* env)]
          (vswap! batch-pending update (::pco/op-name batch-hold) coll/vconj
                  batch-hold)
          nil)

        (not (refs/kw-identical? ::node-error response))
        (do
          (merge-resolver-response! env response)
          (run-next-node! env node))))))

(>defn run-or-node!
  [{::pcp/keys [graph] :as env} {::pcp/keys [run-or] :as or-node}]
  [(s/keys :req [::pcp/graph]) ::pcp/node => nil?]
  (loop [nodes run-or]
    (let [[node-id & tail] nodes]
      (when node-id
        (run-node! env (pcp/get-node graph node-id))
        (if-not (all-requires-ready? env or-node)
          (recur tail)))))
  (run-next-node! env or-node))

(>defn run-and-node!
  "Given an AND node, runs every attached node, then runs the attached next."
  [{::pcp/keys [graph] :as env} {::pcp/keys [run-and] :as and-node}]
  [(s/keys :req [::pcp/graph]) ::pcp/node => nil?]
  (doseq [node-id run-and]
    (run-node! env (pcp/get-node graph node-id)))
  (run-next-node! env and-node))

(>defn run-node!
  "Run a node from the compute graph. This will start the processing on the sent node
  and them will run everything that's connected to this node as sequences of it.

  The result is going to build up at ::p.ent/cache-tree*, after the run is concluded
  the output will be there."
  [env node]
  [(s/keys :req [::pcp/graph ::p.ent/entity-tree*]) ::pcp/node
   => nil?]
  (merge-node-stats! env node
                     {::node-run-start (time/now-ms)})

  (case (pcp/node-kind node)
    ::pcp/node-resolver
    (run-resolver-node! env node)

    ::pcp/node-and
    (run-and-node! env node)

    ::pcp/node-or
    (run-or-node! env node)

    nil))

(defn placeholder-merge-entity
  "Create an entity to process the placeholder demands. This consider if the placeholder
  has params, params in placeholders means that you want some specific data at that
  point."
  [{::pcp/keys [graph]} source-ent]
  (reduce
    (fn [out ph]
      (let [data (:params (pcp/entry-ast graph ph))]
        (assoc out ph
          ; TODO maybe check for possible optimization when there are no conflicts
          ; between different placeholder levels
          (merge source-ent data))))
    {}
    (::pcp/placeholders graph)))

(defn invoke-mutation!
  "Run mutation from AST."
  [env {:keys [key params]}]
  (let [mutation (pci/mutation env key)
        result   (try
                   (pco.prot/-mutate mutation env params)
                   (catch #?(:clj Throwable :cljs :default) e
                     {::mutation-error e}))]
    (p.ent/swap-entity! env assoc key
      (process-attr-subquery env {} key result))))

(defn process-mutations!
  "Runs the mutations gathered by the planner."
  [{::pcp/keys [graph] :as env}]
  (doseq [ast (::pcp/mutations graph)]
    (p.plugin/run-with-plugins env ::wrap-mutate
      invoke-mutation! env ast)))

(>defn run-graph!*
  "Run the root node of the graph. As resolvers run, the result will be add to the
  entity cache tree."
  [{::pcp/keys [graph] :as env}]
  [(s/keys :req [::pcp/graph ::p.ent/entity-tree*])
   => (s/keys)]
  (let [source-ent (p.ent/entity env)]
    ; mutations
    (process-mutations! env)

    ; compute nested available fields
    (if-let [nested (::pcp/nested-available-process graph)]
      (merge-resolver-response! env (select-keys (p.ent/entity env) nested)))

    ; process idents
    (if-let [idents (::pcp/idents graph)]
      (process-idents! env idents))

    ; now run the nodes
    (if-let [root (pcp/get-root-node graph)]
      (run-node! env root))

    ; placeholders
    (merge-resolver-response! env (placeholder-merge-entity env source-ent))

    graph))

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
                    ::compute-plan-run-start-ms start-plan
                    ::compute-plan-run-finish-ms finish-plan)))]
    (run-graph!*
      (assoc env
        ::pcp/graph graph
        ::p.ent/entity-tree* entity-tree*))))

(defn assoc-end-plan-stats [env plan]
  (assoc plan
    ::graph-run-start-ms (::graph-run-start-ms env)
    ::graph-run-finish-ms (time/now-ms)
    ::node-run-stats (some-> env ::node-run-stats* deref)))

(defn run-batches! [env]
  (let [batches* (-> env ::batch-pending*)
        batches  @batches*]
    (vreset! batches* {})
    (doseq [[batch-op batch-items] batches]
      (let [inputs    (mapv ::node-run-input batch-items)
            resolver  (pci/resolver env batch-op)
            batch-env (-> batch-items first ::env
                          (coll/update-if ::p.path/path #(cond-> % (seq %) pop)))
            start     (time/now-ms)
            responses (try
                        (pco.prot/-resolve resolver batch-env inputs)
                        (catch #?(:clj Throwable :cljs :default) e
                          (p.plugin/run-with-plugins env ::wrap-batch-resolver-error
                            (fn [_ _ _]) env [batch-op batch-items] e)

                          (doseq [{env'       ::env
                                   ::pcp/keys [node]} batch-items]
                            (p.plugin/run-with-plugins env' ::wrap-resolver-error
                              mark-resolver-error env' node (ex-info "Batch error" {::batch-error? true} e)))
                          ::node-error))
            finish    (time/now-ms)]

        (when-not (refs/kw-identical? ::node-error responses)
          (if (not= (count inputs) (count responses))
            (throw (ex-info "Batch results must be a sequence and have the same length as the inputs." {})))

          (doseq [[{env'       ::env
                    :keys      [::node-run-input]
                    ::pco/keys [cache? cache-store]
                    ::pcp/keys [node]
                    :as        batch-item} response] (map vector batch-items responses)]
            (if cache?
              (p.cache/cached cache-store env'
                [batch-op node-run-input (pco/params batch-item)]
                (fn [] response)))
            (merge-node-stats! env' node {::batch-run-start-ms  start
                                          ::batch-run-finish-ms finish
                                          ::node-run-output     response})
            (merge-resolver-response! env' response)
            (run-next-node! env' node)
            (if (seq (::p.path/path env'))
              (p.ent/swap-entity! env assoc-in (::p.path/path env')
                (-> (p.ent/entity env')
                    (vary-meta assoc ::run-stats
                               (assoc-end-plan-stats env' (::pcp/graph env'))))))))))))

(defn run-graph-impl!
  [env ast-or-graph entity-tree*]
  (let [env  (-> env
                 ; due to recursion those need to be defined only on the first time
                 (coll/merge-defaults {::pcp/plan-cache* (volatile! {})
                                       ::batch-pending*  (volatile! {})
                                       ::resolver-cache* (volatile! {})
                                       ::p.path/path     []})
                 ; these need redefinition at each recursive call
                 (assoc
                   ::graph-run-start-ms (time/now-ms)
                   ::p.ent/entity-tree* entity-tree*
                   ::node-run-stats* (volatile! ^::map-container? {})))
        plan (plan-and-run! env ast-or-graph entity-tree*)]

    ; run batches on root path only
    (when (p.path/root? env)
      (while (seq @(::batch-pending* env))
        (run-batches! env)))

    ; return result with run stats in meta
    (-> (p.ent/entity env)
        (vary-meta assoc ::run-stats (assoc-end-plan-stats env plan)))))

(>defn run-graph!
  "Plan and execute a request, given an environment (with indexes), the request AST
  and the entity-tree*."
  [env ast-or-graph entity-tree*]
  [(s/keys) (s/or :ast :edn-query-language.ast/node
                  :graph ::pcp/graph) ::p.ent/entity-tree*
   => (s/keys)]
  (p.plugin/run-with-plugins env ::wrap-run-graph!
    run-graph-impl! env ast-or-graph entity-tree*))

(>defn with-resolver-cache
  ([env] [map? => map?] (with-resolver-cache env (atom {})))
  ([env cache*] [map? p.cache/cache-store? => map?] (assoc env ::resolver-cache* cache*)))
