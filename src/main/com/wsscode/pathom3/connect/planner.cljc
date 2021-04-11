(ns com.wsscode.pathom3.connect.planner
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [>def >defn >fdef => | <- ?]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.misc.refs :as refs]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.cache :as p.cache]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.placeholder :as pph]
    [edn-query-language.core :as eql])
  #?(:cljs
     (:require-macros
       [com.wsscode.pathom3.connect.planner])))

; region specs

(>def ::node-id
  "ID for a execution node in the planner graph."
  pos-int?)

(>def ::node-id-set
  "A set of node ids."
  (s/coll-of ::node-id :kind set?))

(>def ::graph
  "The graph container, requires nodes."
  (s/keys :req [::nodes]))

(>def ::available-data
  "An shape descriptor declaring which data is already available when the planner starts."
  ::pfsd/shape-descriptor)

(>def ::node-parents
  "A set of node-ids containing the direct parents of the current node.
  In regular execution nodes, this is the reverse of ::run-next, but in case of
  immediate children of branch nodes, this points to the branch node."
  ::node-id-set)

(>def ::attr-deps-trail
  "A set containing attributes already in consideration when computing missing dependencies."
  ::p.attr/attributes-set)

(>def ::branch-type
  "The branch type for a branch node, can be AND or OR"
  #{::run-or ::run-and})

(>def ::id-counter
  "An atom with a number, used to get the next node-id when creating new nodes."
  any?)

(>def ::foreign-ast
  "In dynamic resolver nodes, this contains the AST to be sent into the remote"
  :edn-query-language.ast/node)

(>def ::node-type
  "Type of the nde, can be resolver, AND, OR or unknown."
  #{::node-resolver ::node-and ::node-or ::node-unknown})

(>def ::input
  "An IO-MAP description of required inputs to run the node."
  ::pfsd/shape-descriptor)

(>def ::index-attrs
  "A index pointing from attribute to the node that provides its value."
  (s/map-of ::p.attr/attribute ::node-id-set))

(>def ::index-resolver->nodes
  "An index from resolver symbol to a set of execution nodes where its used."
  (s/map-of ::pco/op-name ::node-id-set))

(>def ::node-depth
  "The node depth on the graph, starts on zero."
  nat-int?)

(>def ::node-branch-depth
  "How far the branch depth goes from the current node."
  nat-int?)

(>def ::node-chain-depth
  "The chain depth relative to the current node."
  nat-int?)

(defn ignore-nils [m]
  (into {} (remove (fn [[_ v]] (nil? v))) m))

(>def ::node
  "Node."
  (s/and (s/conformer ignore-nils) (s/keys :opt [::node-id ::run-next ::node-parents ::expects ::input])))

(>def ::nodes
  "The nodes index."
  (s/map-of ::node-id ::node))

(>def ::params
  "Params to be used when executing the resolver node"
  map?)

(>def ::expects
  "An data shape description of what is expected from this execution node to return."
  ::pfsd/shape-descriptor)

(>def ::root
  "A node-id that defines the root in the planner graph."
  ::node-id)

(>def ::run-and
  "Vector containing nodes ids to run in a AND branch."
  ::node-id-set)

(>def ::run-or
  "Vector containing nodes ids to run in a AND branch."
  ::node-id-set)

(>def ::run-next
  "A node-id that points to the next node to run."
  ::node-id)

(>def ::source-for-attrs
  "Set of attributes that are provided by this node."
  ::p.attr/attributes-set)

(>def ::source-sym
  "On dynamic resolvers, this points to the original source resolver in the foreign parser."
  ::pco/op-name)

(>def ::unreachable-paths
  "A shape containing the attributes that can't be reached considering current graph and available data."
  ::pfsd/shape-descriptor)

(>def ::warn
  "Warn message"
  string?)

(>def ::warnings
  "List of warnings generated during the plan process."
  (s/coll-of (s/keys :req [::warn])))

(>def ::conflict-params
  "Set of params that were conflicting during merge."
  ::p.attr/attributes-set)

(>def ::index-ast
  "Index to find the AST for a given property."
  ::pf.eql/prop->ast)

(>def ::mutations
  "A vector with the AST of every mutation that appears in the query."
  (s/coll-of :edn-query-language.ast/node :kind vector?))

(>def ::nested-process
  "Which attributes need further processing due to sub-query requirements."
  ::p.attr/attributes-set)

(>def ::placeholders
  "Placeholder items to nest in."
  ::p.attr/attributes-set)

(>def ::idents
  "Idents collected while scanning query"
  (s/coll-of ::eql/ident :kind set?))

(>def ::plan-cache*
  "Atom containing the cache atom to support cached planning."
  refs/atom?)

(>def ::snapshots*
  "Atom to store each step of the planning process"
  refs/atom?)

; endregion

(declare add-snapshot! compute-run-graph* compute-attribute-graph)

; region node helpers

(defn next-node-id
  "Return the next node ID in the system, its an incremental number"
  [{::keys [id-counter]}]
  (swap! id-counter inc))

(defn new-node [env node-data]
  (assoc node-data ::node-id (next-node-id env)))

(>defn get-node
  ([graph node-id]
   [(s/keys :req [::nodes]) (? ::node-id)
    => (? ::node)]
   (get-in graph [::nodes node-id]))

  ([graph node-id k]
   [(s/keys :req [::nodes]) (? ::node-id) keyword?
    => any?]
   (get-in graph [::nodes node-id k])))

(defn node-with-resolver-config
  "Get the node plus the resolver config, when the node has an op-name. If node is
  not a resolver not it returns nil."
  [graph env {::keys [node-id] :as node'}]
  (let [node (get-node graph node-id)]
    (if-let [config (some->> node ::pco/op-name (pci/resolver-config env))]
      (merge node' node config))))

(defn assoc-node
  "Set attribute k about node-id. Only assoc when node exists, otherwise its a noop."
  [graph node-id k v]
  (if (get-node graph node-id)
    (assoc-in graph [::nodes node-id k] v)
    graph))

(defn update-node
  "Update a given node in a graph, like Clojure native update."
  ([graph node-id k f]
   (if (get-node graph node-id)
     (update-in graph [::nodes node-id k] f)
     graph))
  ([graph node-id k f v]
   (if (get-node graph node-id)
     (update-in graph [::nodes node-id k] f v)
     graph))
  ([graph node-id k f v v2]
   (if (get-node graph node-id)
     (update-in graph [::nodes node-id k] f v v2)
     graph))
  ([graph node-id k f v v2 v3]
   (if (get-node graph node-id)
     (update-in graph [::nodes node-id k] f v v2 v3)
     graph))
  ([graph node-id k f v v2 v3 & args]
   (if (get-node graph node-id)
     (apply update-in graph [::nodes node-id k] f v v2 v3 args)
     graph)))

(defn get-root-node
  "Returns the root node of the graph."
  [{::keys [root] :as graph}]
  (get-node graph root))

(>defn set-root-node
  [graph node-id]
  [(s/keys :req [::nodes]) (? ::node-id)
   => (s/keys :req [::nodes])]
  (if node-id
    (assoc graph ::root node-id)
    (dissoc graph ::root)))

(>defn node-branches
  "Return node branches, which can be the ::run-and or the ::run-or part of the node."
  [node]
  [(? ::node)
   => (? (s/or :and ::run-and :or ::run-or))]
  (or (::run-and node)
      (::run-or node)))

(>defn branch-node?
  "Returns true when the node is a branch node type."
  [node]
  [(? ::node) => boolean?]
  (boolean (node-branches node)))

(>defn node-kind
  "Return a keyword describing the type of the node."
  [node]
  [(? ::node) => ::node-type]
  (cond
    (::pco/op-name node)
    ::node-resolver

    (::run-and node)
    ::node-and

    (::run-or node)
    ::node-or

    :else
    ::node-unknown))

(defn node->label
  "Return a string representation for the node, for resolver nodes this is the
  symbol, branch nodes get AND / OR respectively."
  [node]
  (str
    (or
      (::pco/op-name node)
      (if (::run-and node) "AND")
      (if (::run-or node) "OR"))))

(defn add-node-parent [graph node-id node-parent-id]
  (assert node-parent-id "Tried to add after node with nil value")
  (update-node graph node-id ::node-parents coll/sconj node-parent-id))

(defn remove-node-parent [graph node-id node-parent-id]
  (let [node          (get-node graph node-id)
        node-parents' (disj (::node-parents node #{}) node-parent-id)]
    (if (seq node-parents')
      (assoc-node graph node-id ::node-parents node-parents')
      (if node
        (update-in graph [::nodes node-id] dissoc ::node-parents)
        graph))))

(defn set-node-run-next*
  "Update the node-id run-next value, if run-next is nil the attribute
  will be removed from the map."
  [graph node-id run-next]
  (if run-next
    (assoc-node graph node-id ::run-next run-next)
    (update-in graph [::nodes node-id] dissoc ::run-next)))

(defn set-node-run-next
  "Set the node run next value and add the node-parent counter part. Noop if target
  and run next are the same node."
  ([graph run-next] (set-node-run-next graph (::root graph) run-next))
  ([graph target-node-id run-next]
   (let [{target-run-next ::run-next} (get-node graph target-node-id)
         graph (if target-run-next
                 (remove-node-parent graph target-run-next target-node-id)
                 graph)]
     (cond
       (not run-next)
       (set-node-run-next* graph target-node-id run-next)

       (and run-next (not= target-node-id run-next))
       (-> graph
           (set-node-run-next* target-node-id run-next)
           (add-node-parent run-next target-node-id))

       :else
       graph))))

(defn set-node-source-for-attrs
  [graph {::p.attr/keys [attribute]}]
  (if-let [node-id (::root graph)]
    (-> graph
        (update-in [::nodes node-id ::source-for-attrs] coll/sconj attribute)
        (update-in [::index-attrs attribute] coll/sconj node-id))
    graph))

(defn add-branch-to-node
  [graph target-node-id branch-type new-branch-node-id]
  (-> graph
      (add-node-parent new-branch-node-id target-node-id)
      (update-node target-node-id branch-type coll/sconj new-branch-node-id)))

(defn add-node-branches [graph target-node-id branch-type node-ids]
  (reduce
    (fn [g node-id]
      (add-branch-to-node g target-node-id branch-type node-id))
    graph
    node-ids))

(defn remove-branch-node-parents
  "When node-id is a branch node, remove all node-parents associated from its children."
  [graph node-id]
  (let [node (get-node graph node-id)]
    (if-let [branches (node-branches node)]
      (reduce
        (fn [g n-id]
          (remove-node-parent g n-id node-id))
        graph
        branches)
      graph)))

(defn remove-from-parent-branches [graph {::keys [node-id node-parents]}]
  (reduce
    (fn [g nid]
      (let [n (get-node graph nid)]
        (cond
          (contains? (::run-and n) node-id)
          (update-node graph nid ::run-and disj node-id)

          (contains? (::run-or n) node-id)
          (update-node graph nid ::run-or disj node-id)

          :else
          g)))
    graph
    node-parents))

(defn remove-node
  "Remove a node from the graph. In case of resolver nodes it also removes them
  from the ::index-syms and after node references."
  [graph node-id]
  (let [{::keys [run-next node-parents] :as node} (get-node graph node-id)]
    (assert (if node-parents
              (every? #(not= node-id (-> (get-node graph %) ::run-next))
                node-parents)
              true)
      (str "Tried to remove node " node-id " that still contains references pointing to it. Move
      the run-next references from the pointer nodes before removing it. Also check if
      parent is branch and trying to merge."))
    (-> graph
        (cond->
          (::pco/op-name node)
          (update-in [::index-resolver->nodes (::pco/op-name node)] disj node-id))
        (remove-branch-node-parents node-id)
        (remove-node-parent run-next node-id)
        (remove-from-parent-branches node)
        (update ::nodes dissoc node-id))))

(defn include-node
  "Add new node to the graph, this add the node and the index of in ::index-syms."
  [graph {::keys [node-id] ::pco/keys [op-name] :as node}]
  (-> graph
      (assoc-in [::nodes node-id] node)
      (cond->
        op-name
        (update-in [::index-resolver->nodes op-name] coll/sconj node-id))))

(defn create-root-and [graph env node-ids]
  (if (= 1 (count node-ids))
    (set-root-node graph (first node-ids))
    (let [expects (reduce
                    pfsd/merge-shapes
                    (mapv #(get-node graph % ::expects) node-ids))
          {and-node-id ::node-id
           :as         and-node} (new-node env {::expects expects})]
      (-> graph
          (include-node and-node)
          (add-node-branches and-node-id ::run-and node-ids)
          (set-root-node and-node-id)
          (as-> <>
            (add-snapshot! <> env
                           {::snapshot-event   ::snapshot-create-and
                            ::snapshot-message "Create root AND"
                            ::highlight-nodes  (into #{(::root <>)} node-ids)
                            ::highlight-styles {(::root <>) 1}}))))))

(defn create-root-or
  [graph {::p.attr/keys [attribute] :as env} node-ids]
  (if (= 1 (count node-ids))
    (set-root-node graph (first node-ids))
    (let [{or-node-id ::node-id
           :as        or-node} (new-node env {::expects {attribute {}}})]
      (-> graph
          (include-node or-node)
          (add-node-branches or-node-id ::run-or node-ids)
          (set-root-node or-node-id)
          (as-> <>
            (add-snapshot! <> env
                           {::snapshot-event   ::snapshot-create-or
                            ::snapshot-message "Create root OR"
                            ::highlight-nodes  (into #{(::root <>)} node-ids)
                            ::highlight-styles {(::root <>) 1}}))))))

; endregion

; region graph helpers

(defn add-snapshot!
  ([graph {::keys [snapshots*]} event-details]
   (if snapshots*
     (swap! snapshots* conj (-> graph (dissoc ::source-ast ::available-data)
                                (merge event-details))))
   graph))

(defn base-graph []
  {::nodes {}})

(defn base-env []
  {::id-counter     (atom 0)
   ::available-data {}})

(defn reset-env
  "Restore the original environment sent to run-graph! Use this for nested graphs
  that need a clean environment."
  [env]
  (-> env meta ::original-env
      (with-meta (meta env))))

(defn add-unreachable-attr
  "Add attribute to unreachable list"
  [graph env attr]
  (-> (update graph ::unreachable-paths pfsd/merge-shapes {attr {}})
      (add-snapshot! env {::snapshot-event   ::snapshot-mark-attr-unreachable
                          ::snapshot-message (str "Mark attribute " attr " as unreachable.")})))

(defn add-warning [graph warn]
  (update graph ::warnings coll/vconj warn))

(defn merge-unreachable
  "Copy unreachable attributes and resolvers from discard-graph to target-graph"
  [target-graph {::keys [unreachable-paths]}]
  (cond-> target-graph
    unreachable-paths
    (update ::unreachable-paths pfsd/merge-shapes (or unreachable-paths {}))))

(>defn graph-provides
  "Get a set with all provided attributes from the graph."
  [{::keys [index-attrs]}]
  [(s/keys :req [::index-attrs])
   => ::p.attr/attributes-set]
  (-> index-attrs keys set))

(>defn entry-ast
  "Find AST node a given entry from the source AST."
  [graph k]
  [(s/keys :req [::index-ast]) any?
   => (? :edn-query-language.ast/node)]
  (get-in graph [::index-ast k]))

(defn mark-attribute-process-sub-query
  "Add information about attribute that is present but requires further processing
  due to subquery, this is created so the runner can quickly know which attributes
  need to have the subquery processing done."
  [graph {:keys [key children query]}]
  (if (or children query)
    (update graph ::nested-process coll/sconj key)
    graph))

(defn add-ident-process [graph {:keys [key]}]
  (update graph ::idents coll/sconj key))

(defn add-placeholder-entry [graph attr]
  (update graph ::placeholders coll/sconj attr))

; endregion

; region node traversal

(>defn find-direct-node-successors
  "Direct successors of node, branch nodes and run-next, in case of branch nodes the
  branches will always come before the run-next."
  [{::keys [run-next] :as node}]
  [::node => (s/coll-of ::node-id)]
  (let [branches (node-branches node)]
    (cond-> []
      branches
      (into branches)

      run-next
      (conj run-next))))

(>defn node-ancestors
  "Return all node ancestors. The order of the output will go from closest to farthest
  nodes, like breathing out of the current node."
  [graph node-id]
  [::graph ::node-id
   => (s/coll-of ::node-id :kind vector?)]
  (loop [node-queue (coll/queue [node-id])
         ancestors  []]
    (if-let [node-id' (peek node-queue)]
      (let [{::keys [node-parents]} (get-node graph node-id')]
        (recur
          (into (pop node-queue) node-parents)
          (conj ancestors node-id')))
      ancestors)))

(>defn node-successors
  "Find successor nodes of node-id, node-id is included in the list. This will add
  branch nodes before run-next nodes. Returns a lazy sequence that traverse the graph
  as items are requested."
  [graph node-id]
  [::graph ::node-id => (s/coll-of ::node-id)]
  (let [successors (find-direct-node-successors (get-node graph node-id))]
    (cond
      (seq successors)
      (lazy-seq (cons node-id (apply concat (map #(node-successors graph %) successors))))

      :else
      (lazy-seq [node-id]))))

(>defn find-run-next-descendants
  "Return descendants by walking the run-next"
  [graph {::keys [node-id]}]
  [::graph ::node => (s/coll-of ::node)]
  (let [node (get-node graph node-id)]
    (loop [descendants [node]
           {::keys [run-next]} node]
      (if-let [next (get-node graph run-next)]
        (recur (conj descendants next) next)
        descendants))))

(defn find-leaf-node
  "Traverses all run-next still it reaches a leaf."
  [graph node]
  [::graph ::node => ::node]
  (peek (find-run-next-descendants graph node)))

; endregion

; region path expansion

(defn create-node-for-resolver-call
  "Create a new node representative to run a given resolver."
  [{::keys        [input]
    ::p.attr/keys [attribute]
    ::pco/keys    [op-name]
    ast           :edn-query-language.ast/node
    :as           env}]
  (let [requires   {attribute {}}
        ast-params (:params ast)]
    (cond->
      (new-node env
                {::pco/op-name op-name
                 ::expects     requires
                 ::input       input})

      (seq ast-params)
      (assoc ::params ast-params))))

(defn compute-resolver-leaf
  "For a set of resolvers (the R part of OIR index), create one OR node that branches
  to each option in the set."
  [graph {::keys [input] :as env} resolvers]
  (let [resolver-nodes (into
                         (list)
                         (map #(create-node-for-resolver-call (assoc env ::pco/op-name %)))
                         resolvers)]
    (if (seq resolver-nodes)
      (-> (reduce #(include-node % %2) graph resolver-nodes)
          (create-root-or env (mapv ::node-id resolver-nodes))
          (as-> <>
            (add-snapshot! <> env {::snapshot-message (str "Add nodes for input path " (pr-str input))
                                   ::highlight-nodes  (into #{(::root <>)} (map ::node-id) resolver-nodes)
                                   ::highlight-styles {(::root <>) 1}})))
      (add-snapshot! graph env {::snapshot-message "No reachable resolver found."}))))

(defn resolvers-missing-optionals
  "Merge the optionals from a collection of resolver symbols."
  [{::keys [available-data] :as env} resolvers]
  (->> (transduce
         (map #(pci/resolver-optionals env %))
         pfsd/merge-shapes
         {}
         resolvers)
       (pfsd/missing available-data)))

(defn compute-missing-chain-deps
  [graph {::keys [available-data] :as env} missing]
  (reduce-kv
    (fn [[graph node-map] attr shape]
      (if (contains? available-data attr)
        [(-> graph
             (update-in [::index-ast attr] pf.eql/merge-ast-children
               (-> (pfsd/shape-descriptor->ast shape)
                   (assoc :type :prop :key attr :dispatch-key attr)))
             (mark-attribute-process-sub-query {:key attr :children []}))
         node-map]

        (let [graph' (compute-attribute-graph
                       (dissoc graph ::root)
                       (-> env
                           (dissoc ::p.attr/attribute)
                           (update ::attr-deps-trail coll/sconj (::p.attr/attribute env))
                           (assoc
                             :edn-query-language.ast/node
                             {:type         :prop
                              :key          attr
                              :dispatch-key attr})))]
          (if-let [root (::root graph')]
            [graph' (assoc node-map attr root)]
            (reduced [graph' nil])))))
    [graph {}]
    missing))

(defn compute-missing-chain-optional-deps
  [graph {::keys [available-data] :as env} opt-missing]
  (reduce-kv
    (fn [[graph node-map] attr shape]
      (if (contains? available-data attr)
        [(-> graph
             (update-in [::index-ast attr] pf.eql/merge-ast-children
               (-> (pfsd/shape-descriptor->ast shape)
                   (assoc :type :prop :key attr :dispatch-key attr)))
             (mark-attribute-process-sub-query {:key attr :children []}))
         node-map]

        (let [graph' (compute-attribute-graph
                       (dissoc graph ::root)
                       (-> env
                           (dissoc ::p.attr/attribute)
                           (update ::attr-deps-trail coll/sconj (::p.attr/attribute env))
                           (assoc
                             :edn-query-language.ast/node
                             {:type         :prop
                              :key          attr
                              :dispatch-key attr})))]
          (if-let [root (::root graph')]
            [graph' (assoc node-map attr root)]
            [(merge-unreachable graph graph') node-map]))))
    [graph {}]
    opt-missing))

(defn merge-nested-missing-ast [graph {::keys [resolvers] :as env} missing]
  ; TODO: maybe optimize this in the future with indexes to avoid this scan
  (let [recursive-joins
        (into {}
              (comp (map #(pci/resolver-config env %))
                    (mapcat ::pco/input)
                    (keep (fn [x] (if (and (map? x) (pf.eql/recursive-query? (first (vals x))))
                                    (first x)))))
              resolvers)]
    (reduce-kv
      (fn [g attr shape]
        (if-let [recur (get recursive-joins attr)]
          ;; recursive
          (-> (update-in g [::index-ast attr] assoc
                :type :join
                :key attr
                :dispatch-key attr
                :query recur)
              (mark-attribute-process-sub-query {:key attr :children []}))

          ;; standard
          (update-in g [::index-ast attr] pf.eql/merge-ast-children
            (-> (pfsd/shape-descriptor->ast shape)
                (assoc :type :prop :key attr :dispatch-key attr)))))
      graph
      (->> missing
           (into {} (filter (fn [[k v]] (or (contains? recursive-joins k) (seq v)))))))))

(defn compute-missing-chain
  "Start a recursive call to process the dependencies required by the resolver."
  [graph env missing missing-optionals resolvers]
  (let [_ (add-snapshot! graph env {::snapshot-message (str "Computing " (::p.attr/attribute env) " dependencies: " (pr-str missing))})
        [graph' node-map] (compute-missing-chain-deps graph env missing)]
    (if (some? node-map)
      ;; add new nodes (and maybe nested processes)
      (let [[graph'' node-map-opts] (compute-missing-chain-optional-deps graph' env missing-optionals)
            all-nodes (vals (merge node-map node-map-opts))]
        (-> graph''
            (cond->
              (seq all-nodes)
              (create-root-and env (vals (merge node-map node-map-opts)))

              (empty? all-nodes)
              (set-root-node (::root graph)))
            (merge-nested-missing-ast (assoc env ::resolvers resolvers) (pfsd/merge-shapes missing missing-optionals))
            (as-> <>
              (add-snapshot! <> env {::snapshot-event   ::compute-missing-success
                                     ::snapshot-message (str "Complete computing deps " (pr-str missing))
                                     ::highlight-nodes  (into #{(::root <>)} (vals node-map))
                                     ::highlight-styles {(::root <>) 1}}))))

      ;; failed
      (-> graph
          (dissoc ::root)
          (merge-unreachable graph')
          (add-snapshot! env {::snapshot-event   ::compute-missing-failed
                              ::snapshot-message (str "Failed to compute dependencies " (pr-str missing))})))))

(defn compute-input-resolvers-graph
  "This function computes the graph for a given `process path`. It creates the resolver
  nodes to execute the resolvers, in case of many resolvers it uses a OR node to combine
  them.

  Them it fetches the dependencies, declared in the process path. If the dependencies
  are successfully computed, it returns the graph with the root on the node that
  fulfills the request."
  [graph
   {::keys        [available-data]
    ::p.attr/keys [attribute]
    :as           env}
   input resolvers]
  (if (contains? input attribute)
    ; attribute requires itself, just stop
    graph

    (let [missing      (pfsd/missing available-data input)
          missing-opts (resolvers-missing-optionals env resolvers)
          env          (assoc env ::input input)
          {leaf-root ::root :as graph'} (compute-resolver-leaf graph env resolvers)]
      (if leaf-root
        (if (seq (merge missing missing-opts))
          (let [graph-with-deps (compute-missing-chain graph' env missing missing-opts resolvers)]
            (cond
              (= (::root graph-with-deps) leaf-root)
              (-> graph-with-deps
                  (add-snapshot! env {::snapshot-event   ::snapshot-chained-no-nodes
                                      ::snapshot-message "Chained deps without adding nodes"
                                      ::highlight-nodes  #{leaf-root}}))

              (::root graph-with-deps)
              (let [tail-node-id (::node-id (find-leaf-node graph-with-deps (get-root-node graph-with-deps)))]
                (-> (set-node-run-next graph-with-deps tail-node-id leaf-root)
                    (add-snapshot! env {::snapshot-event   ::snapshot-chained-dependencies
                                        ::snapshot-message "Chained deps"
                                        ::highlight-nodes  (into #{} [tail-node-id leaf-root])})))

              :else
              (-> graph
                  (merge-unreachable graph-with-deps))))
          graph')
        graph))))

(defn compute-attribute-graph*
  "Traverse the attribute options, for example, considering we are processing the
  attribute `:a`. And we have this index:

      {::pci/index-oir {:a {{} #{a}}}}

  This means we are now at the `{{} #{a}}` part, lets call each entry of this map a
  `process path`.

  To break it down, in this case we have one `process path` to get `:a`. Each process
  pair contains an input shape and a set of resolvers.

  You can read it as: I can fetch `:a` providing the data `{}` to the resolver `a`.

  A bigger example:

      {::pci/index-oir {:a {{:b {}} #{a-from-b}
                            {:c {}} #{a-from-c a-f-c}}}}

  In this case we have two process paths.

  This function iterates over each process path, if at least one can complete the path,
  it returns a graph with a root o the node. In case of many options, an OR node will
  be the root, providing each path."
  [graph
   {::pci/keys    [index-oir]
    ::p.attr/keys [attribute]
    :as           env}]
  (let [graph (add-snapshot! graph env {::snapshot-event   ::snapshot-process-attribute
                                        ::snapshot-message (str "Process attribute " attribute)})

        [graph' node-ids]
        (reduce-kv
          (fn [[graph nodes] input resolvers]
            (let [graph' (compute-input-resolvers-graph (dissoc graph ::root) env input resolvers)]
              (if (::root graph')
                [graph' (conj nodes (::root graph'))]
                [(merge-unreachable graph graph') nodes])))
          [graph #{}]
          (get index-oir attribute))]
    (if (seq node-ids)
      (-> (create-root-or graph' env node-ids)
          (set-node-source-for-attrs env))
      (-> graph
          (add-unreachable-attr env attribute)
          (merge-unreachable graph')))))

(defn compute-attribute-graph
  "Compute the run graph for a given attribute."
  [{::keys [unreachable-paths] :as graph}
   {::keys      [attr-deps-trail]
    ::pci/keys  [index-oir]
    {attr :key} :edn-query-language.ast/node
    :as         env}]
  (let [env (assoc env ::p.attr/attribute attr)]
    (cond
      (contains? unreachable-paths attr)
      (add-snapshot! graph env {::snapshot-event   ::snapshot-attribute-unreachable
                                ::snapshot-message (str "Attribute unreachable " attr)})

      (contains? attr-deps-trail attr)
      (add-snapshot! graph env {::snapshot-event   ::snapshot-attribute-cycle-dependency
                                ::snapshot-message (str "Attribute cycle detected for " attr)})

      ; its part of the index, traverse the options. this process also compute the
      ; dependencies for this attribute
      (contains? index-oir attr)
      (compute-attribute-graph* graph env)

      :else
      (add-unreachable-attr graph env attr))))

(defn compute-non-index-attribute
  "This function deals with attributes that are not part of the index execution. The
  cases here are:

  - EQL idents
  - Previously available data
  - Placeholders"
  [graph
   {::keys     [available-data]
    {attr :key
     :as  ast} :edn-query-language.ast/node
    :as        env}]
  (cond
    (eql/ident? attr)
    (-> (add-ident-process graph ast)
        (add-snapshot! env {::snapshot-event   ::snapshot-add-ident-process
                            ::snapshot-message (str "Add ident process for " (pr-str attr))}))

    (contains? available-data attr)
    (-> (mark-attribute-process-sub-query graph ast)
        (add-snapshot! env {::snapshot-event   ::snapshot-attribute-already-available
                            ::snapshot-message (str "Attribute already available " attr)}))

    (pph/placeholder-key? env attr)
    (compute-run-graph* (add-placeholder-entry graph attr) env)))

(defn compute-run-graph*
  "Starts scanning the AST to plan for each attribute."
  [graph env]
  (let [[graph' node-ids]
        (reduce
          (fn [[graph node-ids] ast]
            (cond
              (contains? #{:prop :join} (:type ast))
              (let [env (assoc env :edn-query-language.ast/node ast)]
                (or
                  ; try to compute a non-index attribute
                  (if-let [{::keys [root] :as graph'} (compute-non-index-attribute graph env)]
                    [graph' (cond-> node-ids root (conj root))])

                  ; try to figure the attribute from the indexes
                  (let [{::keys [root] :as graph'}
                        (compute-attribute-graph graph
                          (assoc env :edn-query-language.ast/node ast))]
                    (if root
                      ; success
                      [graph' (conj node-ids root)]
                      ; failed, collect unreachables
                      [(merge-unreachable graph graph') node-ids]))))

              ; process mutation
              (refs/kw-identical? (:type ast) :call)
              [(update graph ::mutations coll/vconj ast) node-ids]

              :else
              [graph node-ids]))
          [graph #{}]
          (:children (:edn-query-language.ast/node env)))]
    (if (seq node-ids)
      (create-root-and graph' env node-ids)
      graph')))

(>defn compute-run-graph
  "Generates a run plan for a given environment, the environment should contain the
  indexes in it (::pc/index-oir and ::pc/index-resolvers). It computes a plan to execute
  one level of an AST, the AST must be provided via the key :edn-query-language.ast/node.

      (compute-run-graph (assoc indexes :edn-query-language.ast/node ...))

  The resulting graph will look like this:

      {::nodes                 {1 {::pco/op-name      a
                                   ::node-id          1
                                   ::requires         {:a {}}
                                   ::input            {}
                                   ::source-for-attrs #{:a}
                                   ::node-parents      #{3}}
                                2 {::pco/op-name      b
                                   ::node-id          2
                                   ::requires         {:b {}}
                                   ::input            {}
                                   ::source-for-attrs #{:b}
                                   ::node-parents      #{3}}
                                3 {::node-id  3
                                   ::requires {:b {} :a {} :c {}}
                                   ::run-and  #{2 1 4}}
                                4 {::pco/op-name      c
                                   ::node-id          4
                                   ::requires         {:c {}}
                                   ::input            {}
                                   ::source-for-attrs #{:c}
                                   ::node-parents      #{3}}}
       ::index-resolver->nodes {a #{1} b #{2} c #{4}}
       ::unreachable-attrs     #{}
       ::index-attrs           {:a #{1} :b #{2} :c #{4}}
       ::root                  3}
  "
  ([env]
   [(s/keys
      :req [:edn-query-language.ast/node]
      :opt [::available-data
            ::pci/index-mutations
            ::pci/index-oir
            ::pci/index-resolvers
            ::plan-cache*])
    => ::graph]
   (compute-run-graph {} env))

  ([graph env]
   [(? (s/keys))
    (s/keys
      :req [:edn-query-language.ast/node]
      :opt [::available-data
            ::pci/index-mutations
            ::pci/index-oir
            ::pci/index-resolvers
            ::plan-cache*])
    => ::graph]
   (add-snapshot! graph env {::snapshot-event   ::snapshot-start-graph
                             ::snapshot-message "Start query plan"})
   (p.cache/cached ::plan-cache* env [(hash (::pci/index-oir env))
                                      (::available-data env)
                                      (:edn-query-language.ast/node env)]
     #(compute-run-graph*
        (merge (base-graph)
               graph
               {::index-ast      (pf.eql/index-ast (:edn-query-language.ast/node env))
                ::source-ast     (:edn-query-language.ast/node env)
                ::available-data (::available-data env)})
        (-> (merge (base-env) env)
            (vary-meta assoc ::original-env env))))))

; endregion

(>defn with-plan-cache
  ([env] [map? => map?] (with-plan-cache env (atom {})))
  ([env cache*] [map? p.cache/cache-store? => map?] (assoc env ::plan-cache* cache*)))

(defn compute-plan-snapshots
  "Run compute graph capturing snapshots, return the snapshots vector in the end."
  [env]
  (let [snapshots* (atom [])
        graph      (try
                     (compute-run-graph
                       (assoc (dissoc env ::plan-cache*) ::snapshots* snapshots*))
                     (catch #?(:clj Throwable :cljs :default) e
                       {::snapshot-message (str "Planning stopped due to an error: " (ex-message e))
                        :error             e}))]
    (conj @snapshots* (assoc graph ::snapshot-message "Completed graph."))))
