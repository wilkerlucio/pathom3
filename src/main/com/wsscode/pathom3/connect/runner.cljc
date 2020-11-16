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
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.path :as p.path]))

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

(defn process-map-subquery
  [env ast v]
  (if (map? v)
    (let [cache-tree* (atom v)]
      (run-graph! env ast cache-tree*)
      @cache-tree*)
    v))

(defn process-sequence-subquery
  [env ast v]
  (into
    (empty v)
    (map #(process-map-subquery env ast %))
    v))

(defn process-map-container-subquery
  "Build a new map where the values are replaced with the map process of the subquery."
  [env ast v]
  (coll/map-vals #(process-map-subquery env ast %) v))

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
    :as        env} k v]
  [(s/keys :opt [:edn-query-language.ast/node]) ::p.path/path-entry any?
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
      v)))

(>defn merge-entity-data
  "Specialized merge versions that work on entity data."
  [env entity new-data]
  [(s/keys :opt [::merge-attribute]) ::p.ent/entity-tree ::p.ent/entity-tree
   => ::p.ent/entity-tree]
  (reduce-kv
    (fn [out k v]
      (if (refs/kw-identical? v ::unknown-value)
        out
        (assoc out k (process-attr-subquery env k v))))
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
      #(assoc % k (process-attr-subquery env k
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
    (swap! node-run-stats* update node-id merge data)))

(defn mark-resolver-error
  [{::keys [node-run-stats*]}
   {::pcp/keys [node-id]}
   error]
  (if node-run-stats*
    (swap! node-run-stats* assoc-in [node-id ::node-error] error)))

(defn call-resolver-from-node
  "Evaluates a resolver using node information.

  When this function runs the resolver, if filters the data to only include the keys
  mentioned by the resolver input. This is important to ensure that the resolver is
  not using some key that came accidentally due to execution order, that would lead to
  brittle executions."
  [env
   {::pco/keys [op-name]
    ::pcp/keys [input]
    :as        node}]
  (let [input-keys (keys input)
        resolver   (pci/resolver env op-name)
        {::pco/keys [op-name]
         ::keys     [cache?]
         :or        {cache? true}} (pco/operation-config resolver)
        env        (assoc env ::pcp/node node)
        entity     (p.ent/entity env)
        input-data (select-keys entity input-keys)
        start      (time/now-ms)
        result     (try
                     (if (< (count input-data) (count input-keys))
                       (throw (ex-info "Insufficient data" {:required  input-keys
                                                            :available (keys input-data)}))
                       (if cache?
                         (p.cache/cached ::resolver-cache* env
                           [op-name input-data]
                           #(pco.prot/-resolve resolver env input-data))
                         (pco.prot/-resolve resolver env input-data)))
                     (catch #?(:clj Throwable :cljs :default) e
                       (mark-resolver-error env node e)
                       ::node-error))
        duration   (- (time/now-ms) start)]
    (merge-node-stats! env node
                       {::run-duration-ms duration
                        ::node-run-input  input-data
                        ::node-run-output result})
    result))

(defn run-resolver-node!
  "This function evaluates the resolver associated with the node.

  First it checks if the expected results from the resolver are already available. In
  case they are, the resolver call is skipped."
  [env node]
  (if (all-requires-ready? env node)
    (run-next-node! env node)
    (let [response (call-resolver-from-node env node)]
      (when (not= ::node-error response)
        (merge-resolver-response! env response)
        (run-next-node! env node)))))

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
  [{::pcp/keys [graph] :as env} source-ent]
  (let [entity (p.ent/entity env)]
    (reduce
      (fn [out ph]
        (let [data (:params (pcp/entry-ast graph ph))]
          (assoc out ph
            (if (seq data)
              (merge source-ent data)
              entity))))
      {}
      (::pcp/placeholders graph))))

(>defn run-graph!*
  "Run the root node of the graph. As resolvers run, the result will be add to the
  entity cache tree."
  [{::pcp/keys [graph] :as env}]
  [(s/keys :req [::pcp/graph ::p.ent/entity-tree*])
   => (s/keys)]
  (let [start      (time/now-ms)
        source-ent (p.ent/entity env)
        env        (-> env
                       (coll/merge-defaults {::p.path/path []})
                       (assoc ::node-run-stats* (atom ^::map-container? {})))]

    ; compute nested available fields
    (if-let [nested (::pcp/nested-available-process graph)]
      (merge-resolver-response! env (select-keys (p.ent/entity env) nested)))

    ; process idents
    (if-let [idents (::pcp/idents graph)]
      (process-idents! env idents))

    ; now run the nodes
    (if-let [root (pcp/get-root-node graph)]
      (run-node! env root))

    (merge-resolver-response! env (placeholder-merge-entity env source-ent))

    ; compute minimal stats
    (let [total-time (- (time/now-ms) start)]
      (assoc graph
        ::graph-process-duration-ms total-time
        ::node-run-stats (some-> env ::node-run-stats* deref)))))

(>defn run-graph!
  "Plan and execute a request, given an environment (with indexes), the request AST
  and the entity-tree*."
  [env ast entity-tree*]
  [(s/keys) :edn-query-language.ast/node ::p.ent/entity-tree*
   => (s/keys)]
  (let [graph (pcp/compute-run-graph
                (assoc env
                  :edn-query-language.ast/node ast
                  ::pcp/available-data (pfsd/data->shape-descriptor @entity-tree*)))]
    (run-graph!*
      (-> env
          (coll/merge-defaults
            {::resolver-cache* (atom {})})
          (assoc
            ::pcp/graph graph
            ::p.ent/entity-tree* entity-tree*)))))
