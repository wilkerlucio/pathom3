(ns com.wsscode.pathom3.connect.planner-prev
  (:require
    [clojure.set :as set]
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [>def >defn >fdef => | <- ?]]
    [com.wsscode.log :as l]
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
  (s/map-of ::p.attr/attribute ::node-id))

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

(>def ::graph-before-missing-chain
  "Graph before modifications, this is used to restore previous graph when some path ends up being unreachable."
  ::graph)

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

(>def ::run-next-trail
  "A set containing node ids already in consideration when computing dependencies."
  (s/coll-of ::node-id :kind set?))

(>def ::source-for-attrs
  "Set of attributes that are provided by this node."
  ::p.attr/attributes-set)

(>def ::source-sym
  "On dynamic resolvers, this points to the original source resolver in the foreign parser."
  ::pco/op-name)

(>def ::unreachable-paths
  "A shape containing the attributes that can't be reached considering current graph and available data."
  ::pfsd/shape-descriptor)

(>def ::unreachable-resolvers
  "A set containing the resolvers that can't be reached considering current graph and available data."
  (s/coll-of ::pco/op-name :kind set?))

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

(def pc-sym ::pco/op-name)
(def pc-dyn-sym ::pco/dynamic-name)
(def pc-output ::pco/output)
(def pc-provides ::pco/provides)
(def pc-attr ::p.attr/attribute)
(def pc-input ::pco/input)

(declare
  compute-run-graph compute-run-graph* compute-root-and compute-root-or collapse-nodes-chain node-ancestors
  compute-node-chain-depth collapse-nodes-branch collapse-dynamic-nodes mark-attribute-process-sub-query
  required-input-reachable? find-node-direct-ancestor-chain find-run-next-descendants)

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

(defn next-node-id
  "Return the next node ID in the system, its an incremental number"
  [{::keys [id-counter]}]
  (swap! id-counter inc))

(defn all-node-ids
  "Return all node-ids from the graph."
  [graph]
  (->> graph ::nodes keys))

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

(>defn get-attribute-node
  "Find the node for attribute in attribute index."
  [graph attribute]
  [::graph ::p.attr/attribute => (? ::node-id)]
  (get-in graph [::index-attrs attribute]))

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
    (pc-sym node)
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
      (pc-sym node)
      (if (::run-and node) "AND")
      (if (::run-or node) "OR"))))

(>defn node-parent-run-next
  "Check from the node parents which links using run-next. Return nil when not found."
  [graph {::keys [node-parents node-id]}]
  [::graph (s/keys :req [::node-id] :opt [::node-parents])
   => (? ::node-id)]
  (reduce
    (fn [_ nid]
      (if (= node-id
             (get-node graph nid ::run-next))
        (reduced nid)))
    nil
    node-parents))

(>defn compute-branch-node-chain-depth
  [graph node-id]
  [::graph ::node-id => ::graph]
  (if (get-node graph node-id ::node-branch-depth)
    graph
    (let [branches (node-branches (get-node graph node-id))]
      (if (seq branches)
        (let [graph'       (reduce compute-node-chain-depth graph branches)
              branch-depth (inc (apply max (map #(get-node graph' % ::node-chain-depth) branches)))]
          (assoc-node graph' node-id ::node-branch-depth branch-depth))
        (assoc-node graph node-id ::node-branch-depth 0)))))

(>defn compute-node-chain-depth
  [graph node-id]
  [::graph ::node-id => ::graph]
  (let [{::keys [run-next node-chain-depth] :as node} (get-node graph node-id)
        branches (node-branches node)]
    (cond
      node-chain-depth
      graph

      branches
      (let [graph'       (cond-> (compute-branch-node-chain-depth graph node-id)
                           run-next
                           (compute-node-chain-depth run-next))
            branch-depth (get-node graph' node-id ::node-branch-depth)
            next-depth   (if run-next
                           (inc (get-node graph' run-next ::node-chain-depth))
                           0)]
        (assoc-node graph' node-id ::node-chain-depth (+ branch-depth next-depth)))

      run-next
      (let [graph'     (compute-node-chain-depth graph run-next)
            next-depth (get-node graph' run-next ::node-chain-depth)]
        (assoc-node graph' node-id ::node-chain-depth (inc next-depth)))

      :else
      (assoc-node graph node-id ::node-chain-depth 0))))

(>defn previous-node-depth
  "Get previous node depth, normally it uses the ::node-depth of previous node. There
  is a special case, when the previous node is a branch node and the current node is
  the run-next of the previous, in this case the previous depth should return the node
  chain depth instead, so the depth of the run-next goes after the branch dependencies."
  [graph current-node-id previous-node-id]
  [::graph ::node-id ::node-id
   => ::node-depth]
  (let [previous-node (get-node graph previous-node-id)]
    (if (and (branch-node? previous-node)
             (= current-node-id (::run-next previous-node)))
      (+ (get-node graph previous-node-id ::node-depth)
         (get-node graph previous-node-id ::node-branch-depth))
      (get-node graph previous-node-id ::node-depth))))

(defn compute-node-depth
  "Calculate depth of node-id, this returns a graph in which that node has
  the key ::node-depth associated in the node. The depth is calculated by
  following the ::node-parents chain, in the process all the parent node depths
  are also calculated and set, this makes it efficient to scan the list calculating
  the depths, given each node will be visited at max once."
  [graph node-id]
  (let [{::keys [node-parents node-depth]} (get-node graph node-id)]
    (cond
      node-depth
      graph

      node-parents
      (let [graph' (reduce #(-> (compute-branch-node-chain-depth % %2)
                                (compute-node-depth %2)) graph node-parents)
            depth  (-> (apply max (mapv #(previous-node-depth graph' node-id %) node-parents))
                       inc)]
        (assoc-node graph' node-id ::node-depth depth))

      :else
      (assoc-node graph node-id ::node-depth 0))))

(defn node-depth
  "Compute the depth for node-id and return it."
  [graph node-id]
  (-> (compute-node-depth graph node-id)
      (get-node node-id)
      ::node-depth))

(defn compute-all-node-depths
  "Return graph with node-depth calculated for all nodes."
  [graph]
  (let [node-ids (all-node-ids graph)]
    (reduce
      compute-node-depth
      graph
      node-ids)))

(defn dynamic-resolver?
  [env resolver-name]
  (::pco/dynamic-resolver? (pci/resolver-config env resolver-name)))

(defn add-unreachable-attr
  "Add attribute to unreachable list"
  [graph attr]
  (update graph ::unreachable-paths assoc attr {}))

(defn optimize-merge?
  "Check if node and graph point to same run-next."
  [graph {::keys [node-id]}]
  (let [root-next (get-node graph (::root graph) ::run-next)
        node-next (get-node graph node-id ::run-next)]
    (and root-next (= root-next node-next))))

(defn resolver-provides
  "Get resolver provides from environment source symbol."
  [{::keys [source-sym]
    :as    env}]
  (pci/resolver-provides env source-sym))

(defn find-branch-node-to-merge
  "Given some branch node, tries to find a node with a dynamic resolver that's the
  same sym as the node in node-id."
  [{::keys [root] :as graph}
   {::keys [branch-type]}
   {::keys [node-id]}]
  (let [node     (get-in graph [::nodes node-id])
        node-sym (pc-sym node)]
    (if node-sym
      (some #(if (= node-sym (get-in graph [::nodes % pc-sym])) %)
        (get-in graph [::nodes root branch-type])))))

(defn add-node-parent [graph node-id node-parent-id]
  (assert node-parent-id "Tried to add after node with nil value")
  (update-node graph node-id ::node-parents coll/sconj node-parent-id))

(defn set-node-parents [graph node-id node-parent-id]
  (assert node-parent-id "Tried to set after node with nil value")
  (assoc-node graph node-id ::node-parents #{node-parent-id}))

(defn remove-node-parent [graph node-id node-parent-id]
  (let [node          (get-node graph node-id)
        node-parents' (disj (::node-parents node #{}) node-parent-id)]
    (if (seq node-parents')
      (assoc-node graph node-id ::node-parents node-parents')
      (if node
        (update-in graph [::nodes node-id] dissoc ::node-parents)
        graph))))

(>defn same-resolver?
  "Check if node1 and node2 have the same resolver name."
  [node1 node2]
  [(? map?) (? map?) => boolean?]
  (boolean
    (and (pc-sym node1)
         (= (pc-sym node1) (pc-sym node2)))))

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
  [graph target-node-id run-next]
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
      graph)))

(defn collapse-set-node-run-next
  "Like set-node-run-next, but also checks if the target and next node are the same
  resolver, if so, this fn will collapse those nodes."
  [graph target-node-id run-next]
  (let [target-node (get-node graph target-node-id)
        next-node   (get-node graph run-next)]
    (if (same-resolver? target-node next-node)
      (-> graph
          (collapse-nodes-chain target-node-id run-next))

      (set-node-run-next graph target-node-id run-next))))

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
          (pc-sym node)
          (update-in [::index-resolver->nodes (pc-sym node)] disj node-id))
        (remove-branch-node-parents node-id)
        (remove-node-parent run-next node-id)
        (remove-from-parent-branches node)
        (update ::nodes dissoc node-id))))

(defn merge-node-expects
  "Merge requires from node into target-node-id."
  [graph target-node-id {::keys [expects]}]
  (if expects
    (update-in graph [::nodes target-node-id ::expects] coll/merge-grow expects)
    graph))

(defn merge-node-input
  "Merge input from node into target-node-id."
  [graph target-node-id {::keys [input]}]
  (if input
    (update-in graph [::nodes target-node-id ::input] coll/merge-grow input)
    graph))

(defn add-warning [graph warn]
  (update graph ::warnings coll/vconj warn))

(defn params-conflicting-keys
  "Find conflicting keys between maps m1 and m2, same keys with same values are not
  considered conflicting keys."
  [m1 m2]
  (->> (set/intersection
         (-> m1 keys set)
         (-> m2 keys set))
       (into #{} (remove #(= (get m1 %) (get m2 %))))))

(defn merge-nodes-params
  "Merge params of nodes, in case of conflicting keys a warning will be generated."
  [graph target-node-id {::keys [params]}]
  (if params
    (let [conflict-keys (params-conflicting-keys
                          params
                          (get-in graph [::nodes target-node-id ::params]))]
      (cond-> (update-in graph [::nodes target-node-id ::params] merge params)
        (seq conflict-keys)
        (add-warning {::node-id         target-node-id
                      ::warn            "Conflicting params on resolver call."
                      ::conflict-params conflict-keys})))
    graph))

(defn merge-nodes-foreign-ast
  "Merge the foreign-ast from two dynamic nodes, the operations adds each children from
  node into foreign-ast of the target node. This uses the requires to detect if some
  attribute is already on the query, so this must be called before merging requires to
  get the correct behavior."
  [graph target-node-id {::keys [foreign-ast]}]
  (let [requires (get-node graph target-node-id ::expects)]
    (if foreign-ast
      ; TODO: in case of repeated props, merge params
      (update-in graph [::nodes target-node-id ::foreign-ast :children]
        into (remove (comp requires :key)) (:children foreign-ast))
      graph)))

(defn compute-root-from-branch-type [graph env node]
  (case (::branch-type env)
    ::run-or
    (compute-root-or graph env node)

    (compute-root-and graph env node)))

(defn merge-nodes-run-next
  "Updates target-node-id run-next with the run-next of the last argument. This will do an AND
  branch operation with node-id run-next and run-next, updating the reference of node-id
  run-next."
  [{::keys [root] :as graph} env target-node-id {::keys [run-next]}]
  (let [merge-into-node (get-node graph target-node-id)
        run-next-node   (get-node graph run-next)]
    (if (same-resolver? merge-into-node run-next-node)
      (-> graph
          (add-snapshot! env {::snapshot-message "Collapse dynamic nodes"
                              :compare           [merge-into-node run-next-node]})
          (collapse-dynamic-nodes env target-node-id run-next))
      (-> graph
          (set-root-node (::run-next merge-into-node))
          (compute-root-and env {::node-id run-next})
          (as-> <> (collapse-set-node-run-next <> target-node-id (::root <>)))
          (set-root-node root)))))

(defn transfer-node-source-attrs
  "Pulls source for attributes from node to target-node-id, also updates the attributes
  index to respect the transfer."
  [graph target-node-id {::keys [source-for-attrs]}]
  (if source-for-attrs
    (-> (update-in graph [::nodes target-node-id ::source-for-attrs] (fnil into #{}) source-for-attrs)
        (as-> <>
          (reduce
            #(assoc-in % [::index-attrs %2] target-node-id)
            <>
            source-for-attrs)))
    graph))

(defn transfer-node-parents
  "Set the run next for each after node to be target-node-id."
  [graph target-node-id {::keys [node-parents]}]
  (if (seq node-parents)
    (reduce
      #(set-node-run-next % %2 target-node-id)
      graph
      node-parents)
    graph))

(defn collapse-nodes-branch
  [graph env target-node-id node-id]
  (if (= target-node-id node-id)
    graph
    (let [node (get-node graph node-id)]
      (-> graph
          (merge-nodes-foreign-ast target-node-id node)
          (merge-node-expects target-node-id node)
          (merge-node-input target-node-id node)
          (merge-nodes-params target-node-id node)
          (merge-nodes-run-next env target-node-id node)
          (transfer-node-source-attrs target-node-id node)
          (transfer-node-parents target-node-id node)
          (remove-node node-id)))))

(defn collapse-dynamic-nodes
  [graph env target-node-id node-id]
  (if (= target-node-id node-id)
    graph
    (let [node (get-node graph node-id)]
      (-> graph
          (merge-nodes-foreign-ast target-node-id node)
          (merge-node-expects target-node-id node)
          (merge-nodes-params target-node-id node)
          (merge-nodes-run-next env target-node-id node)
          (transfer-node-source-attrs target-node-id node)
          (transfer-node-parents target-node-id node)
          (remove-node node-id)))))

(defn collapse-nodes-chain
  "Merge chained nodes:

  A -> B

  A is target node, B is the node, this collapses things and only A will exist after."
  [graph target-node-id node-id]
  (if (= target-node-id node-id)
    graph
    (let [node (get-node graph node-id)]
      (-> graph
          (merge-nodes-foreign-ast target-node-id node)
          (merge-node-expects target-node-id node)
          (merge-nodes-params target-node-id node)
          (set-node-run-next target-node-id (::run-next node))
          (transfer-node-parents target-node-id node)
          (transfer-node-source-attrs target-node-id node)
          (remove-node node-id)))))

(defn add-branch-node
  "Given a branch node is the root, this function will add the new node as part
  of that branch node. If the node is a repeating dynamic node it will cause the new node
  to be collapsed into the already existent dynamic node."
  [{::keys [root] :as graph}
   {::keys [branch-type] :as env}
   {::keys [node-id]}]
  (let [node             (get-node graph node-id)
        root-node        (get-node graph root)
        collapse-node-id (find-branch-node-to-merge graph env node)]
    (cond
      collapse-node-id
      (cond-> (collapse-nodes-branch graph env collapse-node-id node-id)
        (refs/kw-identical? branch-type ::run-and)
        (merge-node-expects root (get-node graph node-id)))

      (= (::run-next root-node) node-id)
      graph

      :else
      (if (and (refs/kw-identical? branch-type ::run-and)
               (::run-and node)
               (= (::run-next node)
                  (::run-next root-node)))
        (-> (reduce
              (fn [g node-id]
                (add-branch-node g env {::node-id node-id}))
              graph
              (::run-and node))
            (remove-node node-id))
        (-> graph
            (update-in [::nodes root branch-type] coll/sconj node-id)
            (add-node-parent node-id root)
            (cond->
              (refs/kw-identical? branch-type ::run-and)
              (merge-node-expects root node)))))))

(defn create-branch-node
  [{::keys [root] :as graph} env node branch-node]
  (let [branch-node-id (::node-id branch-node)]
    (-> graph
        (assoc-in [::nodes branch-node-id] branch-node)
        (add-node-parent root branch-node-id)
        (set-root-node branch-node-id)
        (add-branch-node env node))))

(defn branch-add-and-node [graph branch-node-id node-id]
  (-> (update-in graph [::nodes branch-node-id ::run-and] coll/sconj node-id)
      (add-node-parent node-id branch-node-id)))

(defn can-merge-and-nodes? [n1 n2]
  (or (= (::run-next n1) (::run-next n2))
      (and (or (nil? (::run-next n1)) (nil? (::run-next n2)))
           (= (::run-and n1) (::run-and n2)))))

(defn collapse-and-nodes
  "Collapse AND node next-node into AND node target-node-id."
  [graph target-node-id node-id]
  (let [{::keys [run-and run-next] :as node} (get-node graph node-id)
        target-node (get-node graph target-node-id)]
    (assert (can-merge-and-nodes? target-node node)
      "Can't collapse AND nodes with different run-next values.")
    (if (and (::run-and target-node) run-and)
      (-> (reduce
            (fn [graph loop-node-id]
              (branch-add-and-node graph target-node-id loop-node-id))
            graph
            run-and)
          (set-node-run-next* target-node-id (or run-next (::run-next target-node)))
          (transfer-node-parents target-node-id node)
          (remove-node node-id))
      graph)))

(defn compute-root-branch
  [{::keys [root] :as graph}
   {::keys [branch-type] :as env}
   {::keys [node-id]}
   branch-node-factory]
  (if node-id
    (let [root-node       (get-root-node graph)
          next-node       (get-node graph node-id)
          root-sym        (pc-sym root-node)
          next-sym        (pc-sym next-node)
          existent-parent (and root-node next-node
                               (->> (set/intersection
                                      (::node-parents root-node)
                                      (::node-parents next-node))
                                    (filter #(-> (get-node graph %)
                                                 (branch-type %)))
                                    first))]
      (add-snapshot! graph env {::snapshot-message "Start root branch"
                                ::branch-type      branch-type
                                ::highlight-nodes  (into #{} [root node-id])
                                ::root-node        root-node
                                ::next-node        next-node})
      (cond
        ; skip, no next node
        (not next-node)
        graph

        ; skip, not root node
        (not root-node)
        (-> (set-root-node graph node-id)
            (add-snapshot! env {::snapshot-message (str "Set " node-id " as root")}))

        ; same node, collapse
        (and root-sym
             (= root-sym next-sym))
        (-> (add-snapshot! graph env {::snapshot-message (str "Go Collapsed nodes " node-id " and " root " due to same resolver call.")
                                      ::highlight-nodes  (into #{} [node-id root])})
            (collapse-nodes-branch env node-id root)
            (set-root-node node-id)
            (add-snapshot! env {::snapshot-message (str "Collapsed nodes " node-id " and " root " due to same resolver call.")
                                ::highlight-nodes  (into #{} [node-id root])}))

        ; merge ands
        (and (::run-and root-node)
             (::run-and next-node)
             (can-merge-and-nodes? root-node next-node))
        (-> (collapse-and-nodes graph node-id root)
            (set-root-node node-id)
            (add-snapshot! env {::snapshot-message (str "Merged AND nodes " node-id " with " root)
                                ::highlight-nodes  (into #{} [node-id root])}))

        ; next node is branch type
        (and (get next-node branch-type)
             root-sym
             (= (::run-next root-node)
                (::run-next next-node)))
        (-> (add-branch-node (set-root-node graph node-id) env root-node)
            (add-snapshot! env {::snapshot-message (str "Next node is branch, adding a branch there")}))

        ; root node is branch type
        (and (get root-node branch-type)
             next-sym
             (= (::run-next root-node)
                (::run-next next-node)))
        (-> (add-branch-node graph env next-node)
            (add-snapshot! env {::snapshot-message (str "Root node is branch, adding a branch there")}))

        ;; already branch connect
        ; node is already branch connected with root
        (and (branch-type next-node)
             (contains? (branch-type next-node) root))
        (-> (add-snapshot! graph env {::snapshot-message (str "Nodes to join are linked via branch type, keep running")
                                      ::highlight-nodes  (into #{} [node-id root])})
            (set-root-node node-id))

        ; root is already a branch with correct type with the node
        (and (branch-type root-node)
             (contains? (branch-type root-node) node-id))
        (add-snapshot! graph env {::snapshot-message (str "Nodes to join are linked via branch type, keep running")
                                  ::highlight-nodes  (into #{} [node-id root])})

        ; node run next is the root
        (let [ancestors (into #{} (node-ancestors graph root))]
          (contains? ancestors node-id))
        (-> (add-snapshot! graph env {::snapshot-message (str "Nodes to join are linked via run-next, moving root to " node-id)
                                      ::highlight-nodes  (into #{} [node-id root])})
            (set-root-node node-id))

        ; root run next is the node
        (= (::run-next root-node)
           node-id)
        (add-snapshot! graph env {::snapshot-message (str "Nodes to join are linked via run-next, keep running")
                                  ::highlight-nodes  (into #{} [node-id root])})

        ; root and run next are already branches on the same node
        existent-parent
        (-> (add-snapshot! graph env {::snapshot-message (str "Root and node already share parent of correct type, moving root to the parent" existent-parent)
                                      ::highlight-nodes  (into #{} [node-id root existent-parent])
                                      ::highlight-styles {existent-parent 1}})
            (set-root-node existent-parent))

        :else
        (-> (add-snapshot! graph env {::snapshot-message (str "Start branch node " node-id " and " root " using " (name branch-type))
                                      ::branch-type      branch-type
                                      ::highlight-nodes  (into #{} [node-id root])})
            (create-branch-node env next-node (branch-node-factory))
            (add-snapshot! env {::snapshot-message (str "Created new branch node for " node-id " and " root)
                                ::branch-type      branch-type
                                ::highlight-nodes  (into #{} [node-id root])}))))
    graph))

(defn find-nodes-in-between [graph node-id node-id2]
  (let [n1-ancestors (find-node-direct-ancestor-chain graph node-id)
        n2-ancestors (find-node-direct-ancestor-chain graph node-id2)
        dir1         (drop-while #(not= % node-id2) n1-ancestors)
        dir2         (drop-while #(not= % node-id) n2-ancestors)]
    (cond
      (seq dir1)
      dir1

      (seq dir2)
      dir2)))

(defn merge-nested-or [graph node-id]
  (let [{::keys [expects run-or run-next]} (get-node graph node-id)
        nested-or (->> (keep
                         (fn [x]
                           (let [n (get-node graph x)]
                             (if (and (::run-or n)
                                      (= expects (::expects n))
                                      (= run-next (::run-next n)))
                               n)))
                         run-or)
                       first)]
    (if nested-or
      (-> graph
          (update-node node-id ::run-or into (::run-or nested-or))
          (update-node node-id ::run-or disj (::node-id nested-or))
          (as-> <>
            (reduce
              (fn [graph node-id]
                (-> graph
                    (remove-node-parent node-id (::node-id nested-or))
                    (add-node-parent node-id node-id)))
              <>
              (::run-or nested-or)))
          (remove-node (::node-id nested-or)))
      graph)))

(defn convert-and-to-or
  [graph {::p.attr/keys [attribute]} node-id]
  (-> graph
      (update-in [::nodes node-id] set/rename-keys {::run-and ::run-or})
      (update-node node-id ::expects select-keys [attribute])
      (merge-nested-or node-id)))

(defn convert-and-connection-to-or
  [graph env nodes-in-between]
  (reduce
    (fn [g node-id]
      (let [{::keys [run-and]} (get-node graph node-id)]
        (if run-and
          (-> graph
              (convert-and-to-or env node-id)
              (add-snapshot! env {::snapshot-message "Converted AND to OR"
                                  ::highlight-nodes  #{node-id}})
              reduced)
          g)))
    graph
    nodes-in-between))

(defn compute-root-or
  "Combine the root node with a new graph using an OR. This is used to combine nodes
  to allow the runner to choose between paths. This happens when there are multiple
  possibilities to reach some attribute.

  When leave-node-id is present, this means there is a chance the combination nodes
  have the same root, because of attribute re-use. In this case, there will be an AND
  node that should be an OR. In this case the algorithm checks if that's the case, and
  if so it does convert the AND node into an OR node."
  ([{::keys [root] :as graph}
    {::p.attr/keys [attribute] :as env}
    {::keys [node-id] :as node}]
   (cond
     (= root node-id)
     graph

     :else
     (compute-root-branch graph (assoc env ::branch-type ::run-or) node
       (fn []
         {::node-id (next-node-id env)
          ::expects {attribute {}}
          ::run-or  #{(::root graph)}}))))
  ([{::keys [root] :as graph}
    {::p.attr/keys [attribute] :as env}
    {::keys [node-id] :as node}
    leave-node-id]
   (if (and (= root node-id)
            (some? root)
            (-> graph (get-node root) ::expects (contains? attribute) not))
     (let [nodes-between (and root leave-node-id
                              (find-nodes-in-between graph root leave-node-id))]
       (if (seq nodes-between)
         (convert-and-connection-to-or graph env nodes-between)
         graph))
     (compute-root-or graph env node))))

(defn compute-root-and
  [{::keys [root] :as graph} env {::keys [node-id] :as node}]
  (if (= root node-id)
    graph
    (compute-root-branch graph (assoc env ::branch-type ::run-and) node
      (fn []
        (let [{::keys [expects]} (get-root-node graph)]
          {::node-id (next-node-id env)
           ::expects expects
           ::run-and #{(::root graph)}})))))

(def dynamic-base-provider-sym `run-graph-base-provider)

(defn inject-index-nested-provides
  [indexes
   {::p.attr/keys [attribute]
    ::pco/keys    [op-name]
    :as           env}]
  (let [sym-provides    (or (resolver-provides env) {attribute {}})
        nested-provides (get sym-provides attribute)]
    (-> indexes
        (assoc-in [::pci/index-resolvers
                   dynamic-base-provider-sym]
          (pco/resolver
            {pc-sym        dynamic-base-provider-sym
             pc-dyn-sym    op-name
             pc-provides   nested-provides
             ::pco/resolve (fn [_ _])}))
        (update ::pci/index-oir
          (fn [oir]
            (reduce
              (fn [oir attr]
                (update-in oir [attr {}] coll/sconj dynamic-base-provider-sym))
              oir
              (keys nested-provides)))))))

(>defn root-execution-node?
  "A node is a root execution is a node without any ancestors, or all ancestors
  are branch nodes."
  [graph node-id]
  [(s/keys :req [::nodes]) (? ::node-id)
   => boolean?]
  (let [ancestors (node-ancestors graph node-id)
        nodes     (mapv #(get-node graph %) (rest ancestors))]
    (zero? (count (remove branch-node? nodes)))))

(defn compute-nested-node-details
  "Use AST children nodes and resolver provides data to compute the nested requirements
  for dynamic nodes."
  [{ast :edn-query-language.ast/node
    :as env}]
  (let [ast            (pf.eql/maybe-merge-union-ast ast)
        nested-graph   (compute-run-graph*
                         (base-graph)
                         (-> (base-env)
                             (merge (select-keys env [::pci/index-resolvers
                                                      ::pci/index-oir
                                                      ::pci/index-io
                                                      ::pci/index-mutations
                                                      ::pci/index-attributes]))
                             (inject-index-nested-provides env)
                             (assoc :edn-query-language.ast/node ast)))
        sym            (pc-sym env)
        root-dyn-nodes (into []
                             (comp (filter #(root-execution-node? nested-graph %))
                                   (map #(get-node nested-graph %)))
                             (get-in nested-graph [::index-resolver->nodes sym]))
        nodes-inputs   (into []
                             (comp (keep ::run-next)
                                   (map #(get-node nested-graph %))
                                   (keep ::input))
                             root-dyn-nodes)
        dyn-requires   (reduce coll/merge-grow (keep ::expects root-dyn-nodes))
        final-deps     (reduce coll/merge-grow (pfsd/ast->shape-descriptor ast) nodes-inputs)
        children-ast   (-> (first root-dyn-nodes) ::foreign-ast
                           (update :children #(filterv (comp final-deps :key) %))) ; TODO: fix me, consider all root dyn nodes
        ast'           {:type     :root
                        :children [(assoc ast
                                     :query (eql/ast->query children-ast)
                                     :children (:children children-ast))]}]
    {::expects     (select-keys dyn-requires (keys final-deps))
     ::foreign-ast ast'}))

(defn create-resolver-node
  "Create a new node representative to run a given resolver."
  [graph
   {::keys        [run-next input source-sym]
    ::p.attr/keys [attribute]
    ::pco/keys    [op-name]
    ast           :edn-query-language.ast/node
    :as           env}]
  (let [nested     (if (and (seq (:children ast))
                            (dynamic-resolver? env op-name))
                     (compute-nested-node-details env))
        requires   (if nested
                     {attribute (::expects nested)}
                     {attribute {}})
        next-node  (get-node graph run-next)
        ast-params (:params ast)]
    (if (= op-name (pc-sym next-node))
      (-> next-node
          (update ::expects coll/merge-grow requires)
          (assoc ::input input))
      (cond->
        {pc-sym    op-name
         ::node-id (next-node-id env)
         ::expects requires
         ::input   input}

        (seq ast-params)
        (assoc ::params ast-params)

        (dynamic-resolver? env op-name)
        (assoc ::foreign-ast
          (if nested
            (::foreign-ast nested)
            {:type :root :children [ast]}))

        (not= op-name source-sym)
        (assoc ::source-sym source-sym)))))

(defn include-node
  "Add new node to the graph, this add the node and the index of in ::index-syms."
  [graph env {::keys [node-id] :as node}]
  (let [sym (pc-sym node)]
    (-> graph
        (assoc-in [::nodes node-id] node)
        (cond->
          sym
          (update-in [::index-resolver->nodes sym] coll/sconj node-id))
        (add-snapshot! env {::snapshot-event   ::snapshot-include-node
                            ::snapshot-message (str "Included node " sym)
                            ::highlight-nodes  #{node-id}}))))

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

(defn collect-nested-resolver-names
  "From a node, scans all descendents (run next and branches) and collect
  the op-name of every resolver call found."
  ([graph env node] (collect-nested-resolver-names graph env node #{}))
  ([graph
    env
    {::keys [node-id]} syms]
   (let [node (get-in graph [::nodes node-id])]
     (if-let [sym (pc-sym node)]
       (if (dynamic-resolver? env sym)
         syms
         (conj syms sym))
       (into syms (mapcat #(collect-nested-resolver-names graph env {::node-id %}) (find-direct-node-successors node)))))))

(defn all-attribute-resolvers
  "Gets the op name for all resolvers available for a given attribute."
  [{::pci/keys [index-oir]}
   attr]
  (if-let [ir (get index-oir attr)]
    (into #{} cat (vals ir))
    #{}))

(defn mark-node-unreachable
  [previous-graph
   graph
   {::keys [unreachable-resolvers
            unreachable-paths]}
   env]
  (let [syms (->> (collect-nested-resolver-names graph env (get-root-node graph))
                  (into unreachable-resolvers)
                  (into (::unreachable-resolvers previous-graph #{})))]
    (add-snapshot! graph env {::snapshot-message (str "Mark node unreachable, resolvers " (pr-str syms) ", attrs" unreachable-paths)})
    (cond-> (coll/assoc-if previous-graph
                           ::unreachable-resolvers syms
                           ::unreachable-paths unreachable-paths)
      (set/subset? (all-attribute-resolvers env (pc-attr env)) syms)
      (add-unreachable-attr (pc-attr env)))))

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

(>defn node-ancestors-paths
  "Return all node ancestors paths. For when multiple parent paths in a node, there will
  be a new path in the output."
  [graph node-id]
  [::graph ::node-id
   => (s/coll-of (s/coll-of ::node-id :kind vector?) :kind vector?)]
  (loop [node-queue (coll/queue [[node-id]])
         paths      []]
    (if-let [current-path (peek node-queue)]
      (let [node-id' (peek current-path)
            {::keys [node-parents]} (get-node graph node-id')]
        (cond
          (= (count node-parents) 1)
          (let [parent-id (first node-parents)]
            (recur
              (conj (pop node-queue) (conj current-path parent-id))
              paths))

          (> (count node-parents) 1)
          (recur
            (into (pop node-queue) (map #(conj current-path %)) node-parents)
            paths)

          :else
          (recur
            (pop node-queue)
            (conj paths current-path))))
      paths)))

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

(>defn resolver-node-requires-attribute?
  [{::keys [expects sym]} attribute]
  [::node ::p.attr/attribute => boolean?]
  (boolean (and sym (contains? expects attribute))))

(>defn find-attribute-resolver-in-successors
  "Find the nodes that get the data required for the require in the OR node."
  [graph node-id attribute]
  [::graph ::node-id ::p.attr/attribute => ::node-id]
  (->> (node-successors graph node-id)
       (filter #(resolver-node-requires-attribute? (get-node graph %) attribute))
       first))

(defn path-contains-or? [graph path]
  (some (comp ::run-or #(get-node graph %)) path))

(defn first-common-ancestors* [groups]
  (loop [groups     groups
         last-found #{}]
    (let [firsts  (mapv #(into #{} (keep first) %) groups)
          matches (apply set/intersection firsts)]
      (if (and (seq firsts) (seq matches))
        (recur
          (mapv
            #(into [] (keep (fn [x]
                              (if (contains? matches (first x))
                                (rest x)))) %)
            groups)
          matches)
        last-found))))

(>defn first-common-ancestor
  "Find first common node ancestor given a list of node ids."
  [graph node-ids]
  [::graph ::node-id-set => ::node-id]
  (if (= 1 (count node-ids))
    (first node-ids)
    (let [ancestors-path-groups  (into []
                                       (map #(-> graph
                                                 (node-ancestors-paths %)
                                                 (->> (mapv rseq))))
                                       node-ids)

          ; remove the nodes which are parts of other nodes paths
          path-inner-nodes       (apply set/union (mapv (fn [x] (into #{}
                                                                      (comp (map butlast)
                                                                            cat)
                                                                      x)) ancestors-path-groups))
          ancestors-path-groups' (into []
                                       (remove
                                         (fn [x]
                                           (let [item (-> x first last)]
                                             (and (not (branch-node? (get-node graph item)))
                                                  (contains? path-inner-nodes item)))))
                                       ancestors-path-groups)
          ; remove paths containing OR, except if the OR is the end of chain
          ; in case there is no path avoiding the OR in the middle, stick to original
          ; options
          without-or-paths       (mapv #(let [new-seq (into [] (remove (fn [x] (path-contains-or? graph (rest x)))) %)]
                                          (if (seq new-seq)
                                            new-seq
                                            %)) ancestors-path-groups')

          converge-node-id       (or (first (first-common-ancestors* without-or-paths))
                                     (first (first-common-ancestors* ancestors-path-groups')))

          converge-node          (get-node graph converge-node-id)]
      (or (and (branch-node? converge-node)
               (some (into #{} node-ids)
                 (mapv ::node-id
                   (rseq (find-run-next-descendants graph converge-node)))))
          converge-node-id))))

(>defn find-missing-ancestor
  "Find the first common AND node ancestors from missing list, missing is a list
  of attributes"
  [graph missing]
  [::graph ::p.attr/attributes-set
   => ::node-id]
  (if (= 1 (count missing))
    (get-attribute-node graph (first missing))
    (first-common-ancestor graph
                           (into #{} (map (partial get-attribute-node graph)) missing))))

(defn sub-required-input-reachable?*
  [env sub available]
  (let [graph' (compute-run-graph
                 (-> (reset-env env)
                     (assoc
                       ::available-data available
                       :edn-query-language.ast/node (pfsd/shape-descriptor->ast sub))))]
    (every? #(required-input-reachable? graph' env %)
      (pfsd/missing available sub))))

(defn sub-required-input-reachable?
  [{::keys [index-attrs] :as graph} {::keys [available-data] :as env} attr sub]
  (let [available-sub-data (get available-data attr {})]
    (if-let [node-id (get index-attrs attr)]
      (let [resolver   (->> (get-node graph node-id ::pco/op-name)
                            (pci/resolver-config env))
            provides   (->> resolver
                            ::pco/provides
                            attr)
            available  (pfsd/merge-shapes available-sub-data provides)
            reachable? (sub-required-input-reachable?* env sub available)]
        (if-not reachable?
          (l/warn ::nested-input-not-reachable-from-resolver
                  {::l/message   "A nested input wasn't fulfilled. A common issue is that a resolver output is missing the nested description."
                   ::pco/op-name (::pco/op-name resolver)}))
        reachable?)

      ; there is no node from the graph, but still gonna look in current data
      (if (contains? available-data attr)
        (sub-required-input-reachable?* env sub available-sub-data)
        false))))

(defn required-input-reachable?
  "After running a sub graph for dependencies, the planner checks if all required inputs
  are met in the new sub graph. In case of nested dependencies, a new graph must run
  to verify that the sub query dependencies are possible to met, otherwise the path
  is discarded."
  [{::keys [index-attrs] :as graph} env required]
  (let [attr (key required)
        sub  (val required)]
    (if (seq sub)
      ; check if subquery is realizable
      (sub-required-input-reachable? graph env attr sub)
      (contains? index-attrs attr))))

(defn unreachable-attrs-after-missing-check
  "Mark which attributes are unreachable, given the unreachable resolvers.

  This makes sure we don't mark an attribute as unreachable when it still have possibilities
  to run.

  A special case is nested requirements, since they were already part of a separated
  computation we can always merge those in."
  [env unreachable-resolvers still-missing]
  (into {}
        (filter (fn [x]
                  (or (seq (val x))
                      (set/subset? (all-attribute-resolvers env (key x)) unreachable-resolvers))))
        still-missing))

(defn attributes-with-nodes
  "Filter attributes that have attached nodes."
  [{::keys [index-attrs]} attrs]
  (into #{} (filter #(contains? index-attrs %)) attrs))

(defn merge-nested-missing-ast [graph {::keys [available-data resolvers] :as env} missing]
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
        (let [recur (get recursive-joins attr)]
          (if recur
            (-> (update-in g [::index-ast attr] assoc
                  :type :join
                  :key attr
                  :dispatch-key attr
                  :query recur)
                (mark-attribute-process-sub-query {:key attr :children []}))

            (cond-> (update-in g [::index-ast attr] pf.eql/merge-ast-children
                      (-> (pfsd/shape-descriptor->ast shape)
                          (assoc :type :prop :key attr :dispatch-key attr)))
              (contains? available-data attr)
              (mark-attribute-process-sub-query {:key attr :children []})))))
      graph
      (->> missing
           (into {} (filter (fn [[k v]] (or (contains? recursive-joins k) (seq v)))))))))

(defn merge-missing-chain
  "This function will attach the node to its dependencies. The missing chain is the chain
  of dependencies generated by a subgraph. Here the algorithm will also check for
  nested dependencies."
  [graph graph' {::keys [available-data] :as env} missing missing-flat optionals]
  (let [missing-with-nodes (attributes-with-nodes graph' missing-flat)
        graph'             (merge-nested-missing-ast graph' env (pfsd/missing available-data (pfsd/merge-shapes missing optionals)))]
    (if (seq missing-with-nodes)
      (let [ancestor (find-missing-ancestor graph' missing-with-nodes)]
        (add-snapshot! graph' env {::snapshot-message (str "Missing ancestor lookup for attributes " missing-with-nodes ", pick " ancestor)
                                   ::highlight-nodes  (into #{ancestor} (map (partial get-attribute-node graph')) missing-with-nodes)
                                   ::highlight-styles {ancestor 1}})
        (assert ancestor "Error finding ancestor during missing chain computation")
        (cond-> (merge-nodes-run-next graph' env ancestor {::run-next (::root graph)})
          (::run-and (get-root-node graph'))
          (merge-node-expects (::root graph') {::expects (zipmap missing-flat (repeat {}))})

          true
          (add-snapshot! env {::snapshot-message (str "Chaining dependencies for " (pc-attr env) ", set node " (::root graph) " to run after node " ancestor)
                              ::highlight-nodes  (into #{} [(::root graph) ancestor])})))
      (set-root-node graph' (::root graph)))))

(defn resolvers-optionals
  "Merge the optionals from a collection of resolver symbols."
  [env resolvers]
  (transduce
    (map #(pci/resolver-optionals env %))
    pfsd/merge-shapes
    {}
    resolvers))

(defn compute-missing-chain
  "Start a recursive call to process the dependencies required by the resolver. It
  sets the ::run-next data at the env, it will be used to link the nodes after they
  are created in the process."
  [graph {::keys [graph-before-missing-chain available-data] :as env} missing resolvers]
  (let [optionals    (resolvers-optionals env resolvers)
        missing-flat (into []
                           (remove available-data)
                           (concat
                             (keys missing)
                             (keys optionals)))]
    (if (or (seq missing-flat) (seq missing))
      (let [_             (add-snapshot! graph env {::snapshot-message (str "Computing " (pc-attr env) " dependencies: " (pr-str missing))})
            graph'        (compute-run-graph*
                            (dissoc graph ::root)
                            (-> env
                                (dissoc pc-attr)
                                (update ::run-next-trail coll/sconj (::root graph))
                                (update ::attr-deps-trail coll/sconj (pc-attr env))
                                (assoc :edn-query-language.ast/node (eql/query->ast missing-flat))))
            still-missing (into {} (remove #(required-input-reachable? graph' env %)) missing)
            all-provided? (empty? still-missing)]
        (if all-provided?
          (merge-missing-chain graph graph' (assoc env ::resolvers resolvers) missing missing-flat optionals)
          (let [{::keys [unreachable-resolvers] :as out'} (mark-node-unreachable graph-before-missing-chain graph graph' env)
                unreachable-attrs (unreachable-attrs-after-missing-check env unreachable-resolvers still-missing)]
            (update out' ::unreachable-paths pfsd/merge-shapes unreachable-attrs))))
      graph)))

(defn runner-node-sym
  "Find the runner symbol for a resolver, on normal resolvers that is the resolver symbol,
  but for foreign resolvers it uses its ::p.c.o/dynamic-name."
  [env resolver-name]
  (let [resolver (pci/resolver-config env resolver-name)]
    (or (pc-dyn-sym resolver)
        resolver-name)))

(defn extend-resolver-node
  [{::keys [index-resolver->nodes] :as graph}
   {::p.attr/keys [attribute]
    ast           :edn-query-language.ast/node}
   resolver-sym]
  (let [node-id    (first (get index-resolver->nodes resolver-sym))
        ast-params (:params ast)]
    (-> graph
        (merge-node-expects node-id {::expects {attribute {}}})
        (cond->
          ast-params
          (merge-nodes-params node-id {::params ast-params}))
        (set-root-node node-id))))

(defn compute-resolver-graph
  [{::keys [unreachable-resolvers] :as graph}
   env
   resolver]
  (let [resolver' (runner-node-sym env resolver)]
    (cond
      (contains? unreachable-resolvers resolver')
      graph

      :else
      (let [env  (assoc env pc-sym resolver' ::source-sym resolver)
            node (create-resolver-node graph env)]
        (-> graph
            (include-node env node)
            (compute-root-or env node))))))

(defn compute-input-resolvers-graph
  [graph
   {::keys [available-data]
    :as    env}
   input resolvers]
  (let [missing (pfsd/missing available-data input)
        env     (assoc env ::input input)]
    (if (contains? input (pc-attr env))
      graph
      (as-> graph <>
        (dissoc <> ::root)
        ; resolvers loop
        (reduce
          (fn [graph resolver] (compute-resolver-graph graph env resolver))
          <>
          resolvers)

        (if (::root <>)
          (-> <>
              (compute-missing-chain (assoc env ::graph-before-missing-chain graph) missing resolvers)
              (as-> <i>
                (add-snapshot! <i> env {::snapshot-message (str "Compute OR")
                                        ::highlight-nodes  (into #{} [(::root graph)
                                                                      (::root <>)
                                                                      (::root <i>)])
                                        ::highlight-styles {(::root <>) 1}}))
              (compute-root-or env {::node-id (::root graph)} (::root <>)))
          (set-root-node <> (::root graph)))))))

(defn find-node-for-attribute-in-chain
  "Walks the graph run next chain until it finds the node that's providing the
  attribute."
  [graph
   {::p.attr/keys [attribute] :as env}
   root]
  (loop [node-id root]
    (let [{::keys [run-next expects run-and]} (get-node graph node-id)]
      (cond
        run-and
        (some #(find-node-for-attribute-in-chain graph env %)
          (cond->> run-and
            run-next
            (into [run-next])))

        (contains? expects attribute)
        node-id

        run-next
        (recur run-next)))))

(defn set-node-source-for-attrs
  [graph {::p.attr/keys [attribute] :as env}]
  (if-let [node-id (find-node-for-attribute-in-chain graph env (::root graph))]
    (-> graph
        (update-in [::nodes node-id ::source-for-attrs] coll/sconj attribute)
        (update ::index-attrs assoc attribute node-id))
    graph))

(defn find-node-direct-ancestor-chain
  "Computes ancestor chain for a given node, it only walks as long as there is a single
  parent on the node, if there is a fork (multiple node-parents) it will stop."
  [graph node-id]
  (loop [node-id' node-id
         visited  #{}
         chain    (list)]
    (if (contains? visited node-id')
      (do
        (l/warn ::event-ancestor-cycle-detected
                {:visited  visited
                 ::node-id node-id})
        chain)
      (let [{::keys [node-parents]} (get-node graph node-id')
            next-id (first node-parents)]
        (if (= 1 (count node-parents))
          (recur
            next-id
            (conj visited node-id')
            (conj chain node-id'))
          (conj chain node-id'))))))

(defn find-furthest-ancestor
  "Traverse node node-parent chain and returns the most distant resolver ancestor of node id.
  This only walks though resolver nodes, branch nodes are removed."
  [graph node-id]
  (or (->> (find-node-direct-ancestor-chain graph node-id)
           (remove (comp ::run-and #(get-node graph %)))
           (remove (comp ::run-or #(get-node graph %)))
           first)
      node-id))

(defn find-dependent-ancestor*
  "Traverse path until all required data is fulfilled. Returns node in which the data
  is done. If it can't fulfill all the data, returns nil."
  [graph env required path]
  (let [res (reduce
              (fn [missing nid]
                (let [n       (get-node graph nid)
                      expects (::expects n)
                      input   (pfsd/difference (::input n) (::available-data env))
                      missing (pfsd/merge-shapes missing input)
                      remain  (pfsd/difference missing expects)]
                  (if (seq remain)
                    remain
                    (reduced nid))))
              required
              (rest path))]
    (if (map? res)
      [res (last path)]
      res)))

(defn find-dependent-ancestor
  "For a given node N, find which node is responsible for N dependencies. This is done
  by traversing the parents (but only when its run-next) and find tha latest in the
  chain."
  [graph env node-id]
  (let [paths    (node-ancestors-paths graph node-id)
        required (pfsd/difference (get-node graph node-id ::input) (::available-data env))]
    (if (seq required)
      (let [options      (mapv #(find-dependent-ancestor* graph env required %) paths)
            fulfilled-at (or (some #(if (int? %) %) options)
                             (->> options
                                  (sort-by (comp count first))
                                  first second))]
        (or fulfilled-at
            node-id))
      node-id)))

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

(defn push-root-to-ancestor [graph env node-id]
  (let [ancestor (find-dependent-ancestor graph env node-id)]
    (-> graph
        (add-snapshot! env {::snapshot-message (str "Pushing root to attribute ancestor dependency")
                            ::highlight-nodes  (into #{} [node-id ancestor])
                            ::highlight-styles {ancestor 1}})
        (set-root-node ancestor))))

(defn compute-and-unless-root-is-ancestor [{::keys [root] :as graph} env {::keys [node-id]}]
  (if (= node-id (first (find-node-direct-ancestor-chain graph root)))
    (set-root-node graph node-id)
    (compute-root-and graph env {::node-id node-id})))

(defn compute-attribute-graph*
  [{::keys [root] :as graph}
   {::pci/keys    [index-oir]
    ::p.attr/keys [attribute]
    :as           env}]
  (cond
    (get-attribute-node graph attribute)
    (let [node-id (get-attribute-node graph attribute)]
      (-> graph
          (add-snapshot! env {::snapshot-event   ::snapshot-reuse-attribute
                              ::snapshot-message (str "Associating previous root to attribute " attribute " on node " node-id)
                              ::highlight-nodes  #{node-id}})
          (merge-node-expects node-id {attribute {}})
          (push-root-to-ancestor env node-id)
          (compute-and-unless-root-is-ancestor env {::node-id root})
          (add-snapshot! env {::snapshot-event   ::snapshot-reuse-attribute-merged
                              ::snapshot-message (str "Merged attribute " attribute " on node " node-id " with root " root)
                              ::highlight-nodes  (into #{} [node-id root])})))

    :else
    (let [graph'
          (as-> graph <>
            (add-snapshot! <> env {::snapshot-event   ::snapshot-process-attribute
                                   ::snapshot-message (str "Process attribute " attribute)})
            (dissoc <> ::root)
            (reduce-kv
              (fn [graph input resolvers]
                (compute-input-resolvers-graph graph env input resolvers))
              <>
              (get index-oir attribute))
            (set-node-source-for-attrs <> env))]
      (if (::root graph')
        (compute-root-and graph' env {::node-id root})
        (set-root-node graph' root)))))

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

(defn compute-attribute-graph
  "Compute the run graph for a given attribute."
  [{::keys [unreachable-paths] :as graph}
   {::keys     [available-data attr-deps-trail]
    ::pci/keys [index-oir]
    {attr :key
     :as  ast} :edn-query-language.ast/node
    :as        env}]
  (let [env (assoc env pc-attr attr)]
    (cond
      (eql/ident? attr)
      (-> (add-ident-process graph ast)
          (add-snapshot! env {::snapshot-event   ::snapshot-add-ident-process
                              ::snapshot-message (str "Add ident process for " (pr-str attr))}))

      (contains? available-data attr)
      (-> (mark-attribute-process-sub-query graph ast)
          (add-snapshot! env {::snapshot-event   ::snapshot-mark-process-sub-query
                              ::snapshot-message (str "Mark attribute " attr " to sub-process.")}))

      (or (contains? unreachable-paths attr)
          (contains? attr-deps-trail attr))
      graph

      (contains? index-oir attr)
      (compute-attribute-graph* graph env)

      (pph/placeholder-key? env attr)
      (compute-run-graph* (add-placeholder-entry graph attr) env)

      :else
      (-> (add-unreachable-attr graph attr)
          (add-snapshot! env {::snapshot-event   ::snapshot-mark-attr-unreachable
                              ::snapshot-message (str "Mark attribute " attr " as unreachable.")})))))

(defn compute-run-graph*
  [graph env]
  (reduce
    (fn [graph ast]
      (cond
        (contains? #{:prop :join} (:type ast))
        (compute-attribute-graph graph
          (assoc env :edn-query-language.ast/node ast))

        (refs/kw-identical? (:type ast) :call)
        (update graph ::mutations coll/vconj ast)

        :else
        graph))
    graph
    (:children (:edn-query-language.ast/node env))))

(>defn compute-run-graph
  "Generates a run plan for a given environment, the environment should contain the
  indexes in it (::pc/index-oir and ::pc/index-resolvers). It computes a plan to execute
  one level of an AST, the AST must be provided via the key :edn-query-language.ast/node.

      (compute-run-graph (assoc indexes :edn-query-language.ast/node ...))

  The resulting graph will look like this:

      {::nodes                 {1 {::pco/op-name         a
                                   ::node-id          1
                                   ::requires         {:a {}}
                                   ::input            {}
                                   ::source-for-attrs #{:a}
                                   ::node-parents      #{3}}
                                2 {::pco/op-name          b
                                   ::node-id          2
                                   ::requires         {:b {}}
                                   ::input            {}
                                   ::source-for-attrs #{:b}
                                   ::node-parents      #{3}}
                                3 {::node-id  3
                                   ::requires {:b {} :a {} :c {}}
                                   ::run-and  #{2 1 4}}
                                4 {::pco/op-name          c
                                   ::node-id          4
                                   ::requires         {:c {}}
                                   ::input            {}
                                   ::source-for-attrs #{:c}
                                   ::node-parents      #{3}}}
       ::index-resolver->nodes {a #{1} b #{2} c #{4}}
       ::unreachable-resolvers #{}
       ::unreachable-attrs     #{}
       ::index-attrs           {:a 1 :b 2 :c 4}
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
