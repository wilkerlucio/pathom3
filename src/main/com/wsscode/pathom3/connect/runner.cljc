(ns com.wsscode.pathom3.connect.runner
  (:require
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.log :as l]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.misc.refs :as refs]
    [com.wsscode.misc.time :as time]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.cache :as p.cache]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.operation.protocols :as pco.prot]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.error :as p.error]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.path :as p.path]
    [com.wsscode.pathom3.placeholder :as pph]
    [com.wsscode.pathom3.plugin :as p.plugin]))

(>def ::attribute-errors (s/map-of ::p.attr/attribute any?))

(>def ::choose-path
  "A function to determine which path to take when picking a path for a OR node."
  fn?)

(>def ::batch-error? boolean?)

(>def ::batch-hold
  "A map containing information to trigger a batch for a resolver node."
  (s/keys))

(>def ::batch-pending*
  "Atom with batches pending to run."
  any?)

(>def ::batch-waiting*
  "This are nodes with nested inputs that are waiting for batch processes to finish
  to fulfill nested dependencies before they run."
  any?)

(>def ::batch-run-duration-ms number?)
(>def ::batch-run-finish-ms number?)
(>def ::batch-run-start-ms number?)

(>def ::compute-plan-run-duration-ms number?)
(>def ::compute-plan-run-start-ms number?)
(>def ::compute-plan-run-finish-ms number?)

(>def ::mutation-run-duration-ms number?)
(>def ::mutation-run-start-ms number?)
(>def ::mutation-run-finish-ms number?)

(>def ::env map?)

(>def ::graph-run-duration-ms number?)
(>def ::graph-run-start-ms number?)
(>def ::graph-run-finish-ms number?)

(>def ::map-container? boolean?)
(>def ::merge-attribute fn?)

(>def ::node-error any?)
(>def ::node-run-duration-ms number?)
(>def ::node-run-start-ms number?)
(>def ::node-run-finish-ms number?)
(>def ::node-resolver-input map?)
(>def ::node-resolver-output map?)

(>def ::node-run-stats map?)
(>def ::node-run-stats* any?)

(>def ::nodes-with-error ::pcp/node-id-set)

(>def ::resolver-cache* p.cache/cache-store?)
(>def ::resolver-run-duration-ms number?)
(>def ::resolver-run-start-ms number?)
(>def ::resolver-run-finish-ms number?)

(>def ::run-stats map?)
(>def ::omit-run-stats? boolean?)
(>def ::omit-run-stats-resolver-io? boolean?)

(>def ::source-node-id ::pcp/node-id)

(>def ::taken-paths
  "Set with node-ids for attempted paths in a OR node."
  (s/coll-of ::pcp/node-id :kind vector?))

(>def ::success-path
  "Path that succeed in a OR node."
  ::pcp/node-id)

(>def ::root-query
  "Query used to start the process. Not always available."
  vector?)

(>def ::unsupported-batch?
  "Flag to tell the runner that it can't use batch. This happens when navigating for
  example into a set, in which Pathom can't determine which was the original item
  for replacement."
  boolean?)

(>def ::wrap-batch-resolver-error fn?)
(>def ::wrap-merge-attribute fn?)
(>def ::wrap-mutate fn?)
(>def ::wrap-resolve fn?)
(>def ::wrap-resolver-error fn?)
(>def ::wrap-mutation-error fn?)
(>def ::wrap-run-graph! fn?)
(>def ::wrap-entity-ready! fn?)

(>def ::process-run-start-ms number?)
(>def ::process-run-finish-ms number?)
(>def ::process-run-duration-ms number?)

(>def ::node-run-return (? (s/keys :opt [::batch-hold])))

(>def ::node-done?
  "Flag to mark a node is finished running and should be skipped. Only applicable
  for resolver nodes."
  boolean?)

(>defn all-requires-ready?
  "Check if all requirements from the node are present in the current entity."
  [env {::pcp/keys [expects]}]
  [map? (s/keys :req [::pcp/expects])
   => boolean?]
  (let [entity (p.ent/entity env)]
    (every? #(contains? entity %) (keys expects))))

(declare run-node! run-graph! merge-node-stats! include-meta-stats assoc-end-plan-stats)

(defn union-key-on-data? [{:keys [union-key]} m]
  (contains? m union-key))

(defn process-map-subquery-data [ast m]
  (if-let [{:keys [union-key] :as ast} (pf.eql/pick-union-entry ast m)]
    (let [cache-tree* (p.ent/create-entity
                        (cond-> m
                          union-key
                          (vary-meta assoc ::pf.eql/union-entry-key union-key)))]
      [ast cache-tree*])))

(defn process-map-subquery
  [env ast m]
  (if (and (map? m)
           (not (pco/final-value? m)))
    (if-let [[ast cache-tree*] (process-map-subquery-data ast m)]
      (run-graph! env ast cache-tree*))
    m))

(defn process-sequence-subquery
  [env ast s]
  (if (pco/final-value? s)
    s
    (into
      (empty s)
      (keep-indexed #(p.plugin/run-with-plugins env
                       ::wrap-process-sequence-item
                       process-map-subquery
                       (p.path/append-path env %) ast %2))
      (cond-> s
        (coll/coll-append-at-head? s)
        reverse))))

(defn process-map-container-subquery
  "Build a new map where the values are replaced with the map process of the subquery."
  [env ast m]
  (if (pco/final-value? m)
    m
    (into {}
          (map (fn [x]
                 (coll/make-map-entry
                   (key x)
                   (process-map-subquery (p.path/append-path env (key x)) ast (val x)))))
          m)))

(defn process-map-container?
  "Check if the map should be processed as a map-container, this means the sub-query
  should apply to the map values instead of the map itself.

  This can be dictated by adding the ::pcr/map-container? meta data on the value, or
  requested by the query as part of the param."
  [ast v]
  (or (-> v meta ::map-container?)
      (-> ast :params ::map-container?)))

(defn normalize-ast-recursive-query [{:keys [query] :as ast} graph k]
  (let [children (cond
                   (= '... query)
                   (vec (vals (::pcp/index-ast graph)))

                   (pos-int? query)
                   (-> graph ::pcp/index-ast
                       (update-in [k :query] dec)
                       vals vec)

                   :else
                   (:children ast))]
    (assoc ast :children children)))

(defn entry-ast
  "Get AST entry and pulls recursive query when needed."
  [graph k]
  (-> (pcp/entry-ast graph k)
      (normalize-ast-recursive-query graph k)))

(defn fail-fast [{::p.error/keys [lenient-mode?]} error]
  (if-not lenient-mode? (throw error)))

(>defn process-attr-subquery
  [{::pcp/keys [graph]
    :as        env} entity k v]
  [(s/keys :req [::pcp/graph]) map? ::p.path/path-entry any?
   => any?]
  (let [{:keys [children] :as ast} (entry-ast graph k)
        env (p.path/append-path env k)]
    (if children
      (cond
        (map? v)
        (if (process-map-container? ast v)
          (process-map-container-subquery env ast v)
          (process-map-subquery env ast v))

        (coll/collection? v)
        (process-sequence-subquery
          (cond-> env
            ; no batch in sequences that are not vectors because we can't reach those
            ; paths for updating later
            (not (vector? v))
            (assoc ::unsupported-batch? true))
          ast v)

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
    (refs/gswap! node-run-stats* update node-id coll/merge-defaults data)))

(defn resolver-already-ran?
  [{::keys [node-run-stats*]}
   {::pcp/keys [node-id]}]
  (some-> node-run-stats* deref (get node-id) ::node-done?))

(defn merge-mutation-stats!
  [{::keys [node-run-stats*]}
   {::pco/keys [op-name]}
   data]
  (if node-run-stats*
    (refs/gswap! node-run-stats* update op-name coll/merge-defaults data)))

(defn mark-node-error
  [{::keys [node-run-stats*] :as env}
   {::pcp/keys [node-id]}
   error]
  (if node-run-stats*
    (doto node-run-stats*
      (refs/gswap! assoc-in [node-id ::node-error] (p.error/datafy-processor-error error))
      (refs/gswap! assoc-in [node-id ::node-done?] true)
      (refs/gswap! update ::nodes-with-error coll/sconj node-id)))
  (fail-fast env error)
  ::node-error)

(defn mark-node-error-with-plugins
  [env node e]
  (p.plugin/run-with-plugins env ::wrap-resolver-error
    mark-node-error env node e))

(defn choose-cache-store [env cache-store]
  (if cache-store
    (if (contains? env cache-store)
      cache-store
      (do
        (l/warn ::event-attempt-use-undefined-cache-store
                {::pco/cache-store cache-store})
        ::resolver-cache*))
    ::resolver-cache*))

(defn report-resolver-io-stats
  [{::keys [omit-run-stats-resolver-io?]} input-data result]
  (if omit-run-stats-resolver-io?
    {::node-resolver-input-shape  (pfsd/data->shape-descriptor input-data)
     ::node-resolver-output-shape (pfsd/data->shape-descriptor result)
     ::node-done?                 true}

    {::node-resolver-input  input-data
     ::node-resolver-output (if (::batch-hold result)
                              ::batch-hold
                              result)
     ::node-done?           true}))

(defn missing-maybe-in-pending-batch?
  "Check if there is any pending batching in the sub-path of the current input.

  During the serial execution, a nested input process may be halted waiting to run
  after the all entities pass. In this case we need to also halt the execution to wait
  for that dependent input batch to run before moving on to process this node."
  [{::p.path/keys [path] :as env} input]
  (let [nested-inputs (coll/filter-vals seq input)]
    (if (seq nested-inputs)
      (->> env ::batch-pending* deref vals
           (into [] cat)
           (map (comp
                  #(subvec % (count path))
                  ::p.path/path ::env))
           (some
             (fn [path']
               (contains? nested-inputs (first path')))))
      false)))

(defn wait-batch-response [env node]
  {::batch-hold {::env             env
                 ::pcp/node        node
                 ::nested-waiting? true}})

(defn invoke-resolver-with-plugins [resolver env input-data]
  (p.plugin/run-with-plugins env ::wrap-resolve #(pco.prot/-resolve resolver % %2) env input-data))

(defn cache-key [env input-data op-name params]
  (let [{::pco/keys [cache-key]} (pci/resolver-config env op-name)]
    (if cache-key
      (cache-key env input-data)
      [op-name input-data params])))

(defn- invoke-resolver-cached
  [env cache? op-name resolver cache-store input-data params]
  (if cache?
    (p.cache/cached cache-store env
      (cache-key env input-data op-name params)
      #(invoke-resolver-with-plugins resolver env input-data))

    (invoke-resolver-with-plugins resolver env input-data)))

(defn warn-batch-unsupported [env op-name]
  (l/warn ::event-batch-unsupported
          {::p.path/path (::p.path/path env)
           ::pco/op-name op-name}))

(defn- invoke-resolver-cached-batch
  [env cache? op-name resolver cache-store input-data params]
  (warn-batch-unsupported env op-name)
  (if cache?
    (p.cache/cached cache-store env
      (cache-key env input-data op-name params)
      #(first (invoke-resolver-with-plugins resolver env [input-data])))

    (first (invoke-resolver-with-plugins resolver env [input-data]))))

(defn batch-hold-token
  [env cache? op-name node cache-store input-data params]
  {::batch-hold {::pco/op-name         op-name
                 ::pcp/node            node
                 ::pco/cache?          cache?
                 ::pco/cache-store     cache-store
                 ::pcp/params          params
                 ::node-resolver-input input-data
                 ::env                 env}})

(defn valid-resolver-response? [x]
  (or (map? x)
      (nil? x)))

(defn special-resolver-signal? [response]
  (or (refs/kw-identical? response ::node-error)
      (and (map? response) (contains? response ::batch-hold))))

(defn validate-response!
  [env {::pco/keys [op-name]
        :as        node} response]
  (if (or (special-resolver-signal? response)
          (valid-resolver-response? response))
    response
    (mark-node-error-with-plugins env node (ex-info (str "Resolver " op-name " returned an invalid response: " (pr-str response)) {:response response}))))

(defn report-resolver-error
  [{::p.path/keys  [path]
    ::p.error/keys [lenient-mode?]
    :as            env}
   {::pco/keys [op-name]
    :as        node}
   error]
  (mark-node-error-with-plugins env node
                                (if lenient-mode?
                                  error
                                  (ex-info (str "Resolver " op-name " exception at path " (pr-str path) ": " (ex-message error))
                                           {::p.path/path path} error))))

(defn enhance-dynamic-input
  [{::pco/keys [dynamic-resolver?]} node input-data]
  (if dynamic-resolver?
    {::node-resolver-input input-data
     ::pcp/foreign-ast     (::pcp/foreign-ast node)}
    input-data))

(defn wait-batch-check [env node entity input input+opts op-name input-data]
  (let [missing-all (pfsd/missing-from-data entity input+opts)
        wait-batch? (and missing-all
                         (missing-maybe-in-pending-batch? env input+opts))
        missing     (and missing-all
                         (not wait-batch?)
                         (pfsd/missing-from-data entity input))]
    (cond
      wait-batch?
      (wait-batch-response env node)

      missing
      (report-resolver-error
        (assoc env ::p.error/lenient-mode? true)
        node
        (ex-info (str "Insufficient data calling resolver '" op-name ". Missing attrs " (str/join "," (keys missing)))
                 {:required  input
                  :available (pfsd/data->shape-descriptor input-data)
                  :missing   missing})))))

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
        input+opts      (pfsd/merge-shapes input optionals)
        input-data      (pfsd/select-shape-filtering entity input+opts input)
        input-data      (enhance-dynamic-input r-config node input-data)
        params          (pco/params env)
        cache-store     (choose-cache-store env cache-store)
        resolver-cache* (get env cache-store)
        _               (merge-node-stats! env node
                          {::resolver-run-start-ms (time/now-ms)})
        response        (try
                          (let [batch-check (wait-batch-check env node entity input input+opts op-name input-data)]
                            (cond
                              batch-check
                              batch-check

                              batch?
                              (if-let [x (p.cache/cache-find resolver-cache* [op-name input-data params])]
                                (val x)
                                (if (::unsupported-batch? env)
                                  (invoke-resolver-cached-batch
                                    env cache? op-name resolver cache-store input-data params)
                                  (batch-hold-token env cache? op-name node cache-store input-data params)))

                              :else
                              (invoke-resolver-cached
                                env cache? op-name resolver cache-store input-data params)))
                          (catch #?(:clj Throwable :cljs :default) e
                            (report-resolver-error env node e)))
        finish          (time/now-ms)
        response        (validate-response! env node response)]
    (merge-node-stats! env node
      (cond-> {::resolver-run-finish-ms finish}
        (not (::batch-hold response))
        (merge (report-resolver-io-stats env input-data response))))
    response))

(defn run-resolver-node!
  "This function evaluates the resolver associated with the node.

  First it checks if the expected results from the resolver are already available. In
  case they are, the resolver call is skipped."
  [env node]
  (if (or (resolver-already-ran? env node) (all-requires-ready? env node))
    (run-next-node! env node)
    (let [_ (merge-node-stats! env node {::node-run-start-ms (time/now-ms)})
          {::keys [batch-hold] :as response}
          (invoke-resolver-from-node env node)]
      (cond
        ; propagate batch hold up, this will make all nodes to stop running
        ; so they can wait for the batch result
        batch-hold response

        (not (refs/kw-identical? ::node-error response))
        (do
          (merge-resolver-response! env response)
          (merge-node-stats! env node {::node-run-finish-ms (time/now-ms)})
          (run-next-node! env node))

        :else
        (do
          (merge-node-stats! env node {::node-run-finish-ms (time/now-ms)})
          nil)))))

(defn priority-sort
  "Find the node path with the highest priority to run. Returns a set of the candidates
  with the same priority level."
  [{::pcp/keys [graph] :as env} or-node node-ids]
  (let [expects (-> or-node ::pcp/expects keys)
        nodes   (for [id        node-ids
                      successor (pcp/node-successors graph id)
                      :let [config   (pcp/node-with-resolver-config graph env successor)
                            provides (::pco/provides config)]
                      :when (and provides (every? provides expects))]
                  [id config])
        [id config] (apply max-key #(-> % second (::pco/priority 0)) nodes)]
    (if id
      (into #{} (comp (filter #(= (::pco/priority config) (::pco/priority (second %))))
                      (map first)) nodes)
      node-ids)))

(defn node-weight
  "Sums up the weight of a node and its successors"
  [{::pcp/keys [graph]
    ::keys     [resolver-weights*]
    :as        env} node-id]
  (let [{::pcp/keys [run-next] :as node} (pcp/get-node graph node-id)]
    (cond-> (case (pcp/node-kind node)
              ::pcp/node-and
              (transduce (map #(node-weight env %)) + (::pcp/run-and node))

              ::pcp/node-or
              (apply min (mapv #(node-weight env %) (::pcp/run-or node)))

              ::pcp/node-resolver
              (-> @resolver-weights* (get (::pco/op-name node) 1)))
      run-next
      (+ (node-weight env run-next)))))

(defn weight-sort
  "Sorts nodes based on the weight of their paths. The weight is calculated every time
  a resolver runs."
  [{::keys [resolver-weights*] :as env} _or-node candidates]
  (if resolver-weights*
    (apply min-key #(node-weight env %) candidates)
    (first candidates)))

(defn default-choose-path [env or-node node-ids]
  (let [candidates (priority-sort env or-node node-ids)]
    (if (> (count candidates) 1)
      (weight-sort env or-node candidates)
      (first candidates))))

(defn add-taken-path!
  [{::keys [node-run-stats*]} {::pcp/keys [node-id]} taken-path-id]
  (refs/gswap! node-run-stats* update-in [node-id ::taken-paths] coll/vconj taken-path-id))

(defn fail-or-error [or-node errors]
  (ex-info
    (str "All paths from an OR node failed. Expected: " (::pcp/expects or-node) "\n"
         (str/join "\n" (mapv ex-message errors)))
    {:errors errors}))

(defn handle-or-error [env or-node res]
  (let [error (fail-or-error or-node (::or-option-error res))]
    (mark-node-error-with-plugins env or-node error)
    (fail-fast env error)))

(>defn run-or-node!
  [{::pcp/keys [graph]
    ::keys     [choose-path]
    :or        {choose-path default-choose-path}
    :as        env} {::pcp/keys [run-or] :as or-node}]
  [(s/keys :req [::pcp/graph]) ::pcp/node
   => ::node-run-return]
  (merge-node-stats! env or-node {::node-run-start-ms (time/now-ms)})

  (let [res (if-not (all-requires-ready? env or-node)
              (loop [nodes  run-or
                     errors []]
                (if (seq nodes)
                  (let [picked-node-id (choose-path env or-node nodes)
                        node-id        (if (contains? nodes picked-node-id)
                                         picked-node-id
                                         (do
                                           (l/warn ::event-invalid-chosen-path
                                                   {:expected-one-of nodes
                                                    :chosen-attempt  picked-node-id
                                                    :actual-used     (first nodes)})
                                           (first nodes)))]
                    (add-taken-path! env or-node node-id)
                    (let [res (try
                                (run-node! env (pcp/get-node graph node-id))
                                (catch #?(:clj Throwable :cljs :default) e
                                  {::or-option-error e}))]
                      (cond
                        (::batch-hold res)
                        res

                        (::or-option-error res)
                        (recur (disj nodes node-id) (conj errors (::or-option-error res)))

                        :else
                        (if (all-requires-ready? env or-node)
                          (merge-node-stats! env or-node {::success-path node-id})
                          (recur (disj nodes node-id) errors)))))
                  {::or-option-error errors})))]
    (cond
      (::batch-hold res)
      res

      (::or-option-error res)
      (handle-or-error env or-node res)

      :else
      (do
        (merge-node-stats! env or-node {::node-run-finish-ms (time/now-ms)})
        (run-next-node! env or-node)))))

(>defn run-and-node!
  "Given an AND node, runs every attached node, then runs the attached next."
  [{::pcp/keys [graph] :as env} {::pcp/keys [run-and] :as and-node}]
  [(s/keys :req [::pcp/graph]) ::pcp/node
   => ::node-run-return]
  (merge-node-stats! env and-node {::node-run-start-ms (time/now-ms)})

  (let [res (reduce
              (fn [_ node-id]
                (let [node-res (run-node! env (pcp/get-node graph node-id))]
                  (if (::batch-hold node-res)
                    (reduced node-res))))
              nil
              run-and)]
    (if (::batch-hold res)
      res
      (do
        (merge-node-stats! env and-node {::node-run-finish-ms (time/now-ms)})
        (run-next-node! env and-node)))))

(>defn run-node!
  "Run a node from the compute graph. This will start the processing on the sent node
  and them will run everything that's connected to this node as sequences of it.

  The result is going to build up at ::p.ent/cache-tree*, after the run is concluded
  the output will be there."
  [env node]
  [(s/keys :req [::pcp/graph ::p.ent/entity-tree*]) ::pcp/node
   => ::node-run-return]
  (case (pcp/node-kind node)
    ::pcp/node-resolver
    (run-resolver-node! env node)

    ::pcp/node-and
    (run-and-node! env node)

    ::pcp/node-or
    (run-or-node! env node)))

(defn placeholder-merge-entity
  "Create an entity to process the placeholder demands. This consider if the placeholder
  has params, params in placeholders means that you want some specific data at that
  point."
  [{::pcp/keys [graph] ::keys [source-entity]}]
  (reduce
    (fn [out ph]
      (let [data (:params (pcp/entry-ast graph ph))]
        (assoc out ph
          ; TODO maybe check for possible optimization when there are no conflicts
          ; between different placeholder levels
          (merge source-entity data))))
    {}
    (::pcp/placeholders graph)))

(defn run-foreign-mutation
  [env {:keys [key] :as ast}]
  (let [ast      (cond-> ast (not (:children ast)) (dissoc :children))
        mutation (pci/mutation env key)
        {::pco/keys [dynamic-name]} (pco/operation-config mutation)
        foreign  (pci/resolver env dynamic-name)
        {::pco/keys [batch?]} (pco/operation-config foreign)
        ast      (pcp/promote-foreign-ast-children ast)]
    (-> (pco.prot/-resolve
          foreign
          env
          (cond-> {::pcp/foreign-ast {:type :root :children [ast]}}
            batch? vector))
        (get key))))

(defn invoke-mutation!
  "Run mutation from AST."
  [env {:keys [key] :as ast}]
  (let [mutation (pci/mutation env key)
        start    (time/now-ms)
        _        (merge-mutation-stats! env {::pco/op-name key}
                                        {::node-run-start-ms     start
                                         ::mutation-run-start-ms start})
        result   (try
                   (if mutation
                     (if (-> mutation pco/operation-config ::pco/dynamic-name)
                       (run-foreign-mutation env ast)
                       (p.plugin/run-with-plugins env ::wrap-mutate
                         #(pco.prot/-mutate mutation %1 (:params %2)) env ast))
                     (throw (ex-info (str "Mutation " key " not found") {::pco/op-name key})))
                   (catch #?(:clj Throwable :cljs :default) e
                     (p.plugin/run-with-plugins env ::wrap-mutation-error
                       (fn [_ _ _]) env ast e)
                     (fail-fast env e)
                     {::mutation-error e}))]
    (merge-mutation-stats! env {::pco/op-name key}
                           {::mutation-run-finish-ms (time/now-ms)})

    (if (::mutation-error result)
      (p.ent/swap-entity! env assoc key result)
      (p.ent/swap-entity! env assoc key
        (process-attr-subquery env {} key result)))

    (merge-mutation-stats! env {::pco/op-name key}
                           {::node-run-finish-ms (time/now-ms)})))

(defn process-mutations!
  "Runs the mutations gathered by the planner."
  [{::pcp/keys [graph] :as env}]
  (doseq [key (::pcp/mutations graph)]
    (invoke-mutation! env (entry-ast graph key))))

(defn check-entity-requires!
  "Verify if entity contains all required keys from graph index-ast. This is
  shallow check (don't visit nested entities)."
  [{::pcp/keys    [graph]
    ::p.path/keys [path]
    :as           env}]
  (let [entity   (p.ent/entity env)
        expected (zipmap
                   (into []
                         (comp (map :key)
                               (remove #(pph/placeholder-key? env %)))
                         (:children (pcp/required-ast-from-source-ast graph)))
                   (repeat {}))
        missing  (pfsd/missing (pfsd/data->shape-descriptor-shallow entity) expected)]
    (if (seq missing)
      (fail-fast env
                 (ex-info (str
                            "Required attributes missing: " (pr-str (vec (keys missing)))
                            " at path " (pr-str path))
                          {:missing        missing
                           ::p.error/phase ::execute
                           ::p.error/cause ::p.error/missing-output})))))

(defn run-graph-done! [env]
  (check-entity-requires! env)
  (p.ent/swap-entity! env include-meta-stats env)
  (if (::p.error/lenient-mode? env)
    (p.ent/swap-entity! env #(p.error/process-entity-errors env %)))
  nil)

(defn run-graph-entity-done [env]
  ; placeholders
  (if (-> env ::pcp/graph ::pcp/placeholders)
    (merge-resolver-response! env (placeholder-merge-entity env)))
  ; entity ready
  (p.plugin/run-with-plugins env ::wrap-entity-ready! run-graph-done!
    env))

(defn run-root-node!
  [{::pcp/keys [graph] :as env}]
  (if-let [root (pcp/get-root-node graph)]
    (let [{::keys [batch-hold]} (run-node! env root)]
      (if batch-hold
        (if (::nested-waiting? batch-hold)
          ; add to wait
          (refs/gswap! (::batch-waiting* env) coll/vconj batch-hold)
          ; add to batch pending
          (refs/gswap! (::batch-pending* env) update
                       (select-keys batch-hold [::pco/op-name ::pcp/params])
                       coll/vconj batch-hold))
        (run-graph-entity-done env)))
    (run-graph-entity-done env)))

(>defn run-graph!*
  "Run the root node of the graph. As resolvers run, the result will be add to the
  entity cache tree."
  [{::pcp/keys [graph] :as env}]
  [(s/keys :req [::pcp/graph ::p.ent/entity-tree*])
   => (s/keys)]
  (let [env (assoc env ::source-entity (p.ent/entity env))]
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

    env))

(defn runnable-graph?
  "Quick check to see if the graph has something to run. In the false case we can skip the
  running section."
  [graph]
  (or (seq (::pcp/nodes graph))
      (::pcp/mutations graph)
      (::pcp/nested-process graph)
      (::pcp/idents graph)
      (::pcp/placeholders graph)))

(defn processor-error? [err]
  (some-> (ex-data err) ::processor-error?))

(defn processor-exception [env err]
  (let [env' (assoc env
               ::pcp/graph (assoc-end-plan-stats env)
               ::p.ent/entity-tree (some-> env ::p.ent/entity-tree* deref))]
    (if (processor-error? err)
      (ex-info (ex-message err)
               (-> (ex-data err)
                   (assoc ::processor-error-parent-env env')))
      (ex-info
        (str "Graph execution failed: " (ex-message err))
        (assoc env'
          ::p.error/error-message (ex-message err)
          ::p.error/error-data (ex-data err)
          ::p.error/error-stack (p.error/error-stack err)
          ::processor-error? true)
        err))))

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
                    ::compute-plan-run-finish-ms finish-plan)))
        env   (assoc env
                ::pcp/graph graph
                ::p.ent/entity-tree* entity-tree*)]
    (try
      (if (runnable-graph? graph)
        (run-graph!* env)
        (do
          (run-graph-entity-done env)
          env))
      (catch #?(:clj Throwable :cljs :default) e
        (throw (processor-exception env e))))))

(defn assoc-end-plan-stats [{::pcp/keys [graph] :as env}]
  (assoc graph
    ::graph-run-start-ms (::graph-run-start-ms env)
    ::graph-run-finish-ms (time/now-ms)
    ::node-run-stats (some-> env ::node-run-stats* deref)))

(defn include-meta-stats
  [result {::keys [omit-run-stats?]
           :or    {omit-run-stats? false}
           :as    env}]
  (cond-> result
    (not omit-run-stats?)
    (vary-meta assoc ::run-stats (assoc-end-plan-stats env))))

(defn mark-batch-errors [error env batch-op batch-items]
  (p.plugin/run-with-plugins env ::wrap-batch-resolver-error
    (fn [_ _ _]) env [batch-op batch-items] error)

  (doseq [{env'       ::env
           ::pcp/keys [node]} batch-items]
    (p.plugin/run-with-plugins env' ::wrap-resolver-error
      mark-node-error env' node (ex-info (str "Batch error: " (ex-message error)) {::batch-error? true} error)))

  ::node-error)

(defn cache-batch-item
  [{env'       ::env
    ::keys     [node-resolver-input]
    ::pco/keys [cache? cache-store]
    :as        batch-item}
   batch-op
   response]
  (if cache?
    (p.cache/cached cache-store env'
      (cache-key env' node-resolver-input batch-op (pco/params batch-item))
      (fn [] response))))

(defn combine-inputs-with-responses
  "For batch we group the items with the same inputs so the resolver only needs to have
  each input once. This function is a helper to side the input batch items with the
  distinct list of responses."
  [input-groups inputs responses]
  (->> (mapv input-groups inputs)
       (mapv #(vector %2 %) responses)
       (into []
             (mapcat
               (fn [[inputs result]]
                 (mapv #(vector % result) inputs))))))

(defn merge-entity-to-root-data [env env' node]
  (when-not (p.path/root? env')
    (p.ent/swap-entity! env update-in (::p.path/path env')
      (fn [ent]
        (let [ent' (p.ent/entity env')]
          (-> ent
              (coll/merge-defaults ent')
              (vary-meta merge (meta ent'))
              (merge
                (pfsd/select-shape ent' (assoc (::pcp/expects node)
                                          ::attribute-errors {})))))))))

(defn batch-group-input-groups [batch-items]
  (group-by ::node-resolver-input batch-items))

(defn batch-group-inputs [batch-items]
  (into [] (comp (map ::node-resolver-input) (distinct)) batch-items))

(defn mark-batch-block-errors [err env batch-op input-groups inputs]
  (mark-batch-errors err env batch-op
                     (vec (for [input inputs
                                item  (get input-groups input)]
                            item)))
  (vec (repeat (count inputs) nil)))

(defn invoke-batch-block [resolver batch-env batch-op input-groups inputs]
  (if (::p.error/lenient-mode? batch-env)
    (try
      (invoke-resolver-with-plugins resolver batch-env inputs)
      (catch #?(:clj Throwable :cljs :default) err
        (mark-batch-block-errors err batch-env batch-op input-groups inputs)))
    (invoke-resolver-with-plugins resolver batch-env inputs)))

(defn run-batches-pending! [env]
  (let [batches* (-> env ::batch-pending*)
        batches  @batches*]
    (vreset! batches* {})
    (doseq [[{batch-op ::pco/op-name} batch-items] batches]
      (let [resolver     (pci/resolver env batch-op)
            input-groups (batch-group-input-groups batch-items)
            inputs       (batch-group-inputs batch-items)
            batch-env    (-> batch-items first ::env
                             (coll/update-if ::p.path/path #(cond-> % (seq %) pop)))
            max-size     (-> resolver pco/operation-config ::pco/batch-chunk-size)
            start        (time/now-ms)
            responses    (try
                           (if max-size
                             (into []
                                   (mapcat #(invoke-batch-block
                                              resolver
                                              batch-env
                                              batch-op
                                              input-groups
                                              %))
                                   (partition-all max-size inputs))
                             (invoke-resolver-with-plugins resolver batch-env inputs))
                           (catch #?(:clj Throwable :cljs :default) e
                             (fail-fast env e)
                             (mark-batch-errors e env batch-op batch-items)))
            finish       (time/now-ms)]

        (if (refs/kw-identical? ::node-error responses)
          (if (::p.error/lenient-mode? env)
            (doseq [{env'       ::env
                     ::pcp/keys [node]} batch-items]
              (run-graph-entity-done env')
              (merge-entity-to-root-data env env' node)))
          (do
            (if (not= (count inputs) (count responses))
              (throw (ex-info "Batch results must be a sequence and have the same length as the inputs." {})))

            (doseq [[{env'       ::env
                      ::pcp/keys [node]
                      ::keys     [node-resolver-input]
                      :as        batch-item} response] (combine-inputs-with-responses input-groups inputs responses)]
              (cache-batch-item batch-item batch-op response)

              (merge-node-stats! env' node
                (merge {::batch-run-start-ms  start
                        ::batch-run-finish-ms finish}
                       (report-resolver-io-stats env' node-resolver-input response)))

              (merge-resolver-response! env' response)

              (merge-node-stats! env' node {::node-run-finish-ms (time/now-ms)})

              (run-root-node! env')

              (merge-entity-to-root-data env env' node))))))))

(defn run-batches-waiting! [env]
  (let [waits* (-> env ::batch-waiting*)
        waits  @waits*]
    (vreset! waits* [])
    (doseq [{env' ::env} waits]
      (p.ent/reset-entity! env' (get-in (p.ent/entity env) (::p.path/path env')))

      (run-root-node! env')

      (when-not (p.path/root? env')
        (p.ent/swap-entity! env assoc-in (::p.path/path env')
          (p.ent/entity env'))))))

(defn run-batches! [env]
  (run-batches-pending! env)
  (run-batches-waiting! env))

(defn setup-runner-env [env entity-tree* cache-type]
  (-> env
      ; due to recursion those need to be defined only on the first time
      (coll/merge-defaults {::pcp/plan-cache* (cache-type {})
                            ::batch-pending*  (cache-type {})
                            ::batch-waiting*  (cache-type [])
                            ::resolver-cache* (cache-type {})
                            ::p.path/path     []})
      ; these need redefinition at each recursive call
      (assoc
        ::graph-run-start-ms (time/now-ms)
        ::p.ent/entity-tree* entity-tree*
        ::node-run-stats* (cache-type ^::map-container? {}))))

(defn run-graph-impl!
  [env ast-or-graph entity-tree*]
  (let [env (-> (setup-runner-env env entity-tree* volatile!)
                (plan-and-run! ast-or-graph entity-tree*))]

    (try
      ; run batches on root path only
      (when (p.path/root? env)
        (while (seq @(::batch-pending* env))
          (run-batches! env)))

      ; return result with run stats in meta
      (-> (p.ent/entity env)
          (include-meta-stats env))
      (catch #?(:clj Throwable :cljs :default) e
        (throw (processor-exception env e))))))

(defn run-graph-with-plugins [env ast-or-graph entity-tree* impl!]
  (if (p.path/root? env)
    (p.plugin/run-with-plugins env ::wrap-root-run-graph!
      (fn [e a t]
        (p.plugin/run-with-plugins env ::wrap-run-graph!
          impl! e a t))
      env ast-or-graph entity-tree*)
    (p.plugin/run-with-plugins env ::wrap-run-graph!
      impl! env ast-or-graph entity-tree*)))

(>defn run-graph!
  "Plan and execute a request, given an environment (with indexes), the request AST
  and the entity-tree*."
  [env ast-or-graph entity-tree*]
  [(s/keys) (s/or :ast :edn-query-language.ast/node
                  :graph ::pcp/graph) ::p.ent/entity-tree*
   => (s/keys)]
  (run-graph-with-plugins env ast-or-graph entity-tree* run-graph-impl!))

(>defn with-resolver-cache
  ([env] [map? => map?] (with-resolver-cache env (atom {})))
  ([env cache*] [map? p.cache/cache-store? => map?] (assoc env ::resolver-cache* cache*)))
