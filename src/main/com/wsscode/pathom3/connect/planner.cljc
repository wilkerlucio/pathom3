(ns com.wsscode.pathom3.connect.planner
  (:require
    [clojure.set :as set]
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [>def >defn >fdef => | <- ?]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.misc.refs :as refs]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.cache :as p.cache]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [com.wsscode.pathom3.format.shape-descriptor :as fsd]
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
  ::fsd/shape-descriptor)

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
  ::fsd/shape-descriptor)

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
  ::fsd/shape-descriptor)

(>def ::root
  "A node-id that defines the root in the planner graph."
  ::node-id)

(>def ::run-and
  "Vector containing nodes ids to run in a AND branch."
  ::node-id-set)

(>def ::run-next
  "A node-id that points to the next node to run."
  ::node-id)

(>def ::run-next-trail
  "A set containing node ids already in consideration when computing dependencies."
  (s/coll-of ::node-id :kind set?))

(>def ::run-or
  "Vector containing nodes ids to run in a AND branch."
  ::node-id-set)

(>def ::source-for-attrs
  "Set of attributes that are provided by this node."
  ::p.attr/attributes-set)

(>def ::source-sym
  "On dynamic resolvers, this points to the original source resolver in the foreign parser."
  ::pco/op-name)

(>def ::unreachable-attrs
  "A set containing the attributes that can't be reached considering current graph and available data."
  ::p.attr/attributes-set)

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

(>def ::nested-available-process
  "Which attributes need further processing due to sub-query requirements."
  ::p.attr/attributes-set)

(>def ::idents
  "Idents collected while scanning query"
  (s/coll-of ::eql/ident :kind set?))

(>def ::plan-cache*
  "Atom containing the cache atom to support cached planning."
  refs/atom?)

(def pc-sym ::pco/op-name)
(def pc-dyn-sym ::pco/dynamic-name)
(def pc-output ::pco/output)
(def pc-provides ::pco/provides)
(def pc-attr ::p.attr/attribute)
(def pc-input ::pco/input)

(declare compute-run-graph* compute-root-and collapse-nodes-chain node-ancestors
         compute-node-chain-depth collapse-nodes-branch collapse-dynamic-nodes)

(defn add-snapshot!
  ([graph {::keys [snapshots*]} event-details]
   (if snapshots*
     (swap! snapshots* conj (merge graph event-details)))
   graph))

(defn base-graph []
  {::nodes                 {}
   ::index-resolver->nodes {}
   ::unreachable-resolvers #{}
   ::unreachable-attrs     #{}})

(defn base-env []
  {::id-counter     (atom 0)
   ::available-data {}})

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
  following the ::after-nodes chain, in the process all the parent node depths
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
  (update graph ::unreachable-attrs conj attr))

(defn optimize-merge?
  "Check if node and graph point to same run-next."
  [graph {::keys [node-id]}]
  (let [root-next (get-in graph [::nodes (::root graph) ::run-next])
        node-next (get-in graph [::nodes node-id ::run-next])]
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

(defn add-after-node [graph node-id after-node-id]
  (assert after-node-id "Tried to add after node with nil value")
  (update-node graph node-id ::node-parents coll/sconj after-node-id))

(defn set-after-node [graph node-id after-node-id]
  (assert after-node-id "Tried to set after node with nil value")
  (assoc-node graph node-id ::node-parents #{after-node-id}))

(defn remove-after-node [graph node-id after-node-id]
  (let [node         (get-node graph node-id)
        after-nodes' (disj (::node-parents node #{}) after-node-id)]
    (if (seq after-nodes')
      (assoc-node graph node-id ::node-parents after-nodes')
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
  "Set the node run next value and add the after-node counter part. Noop if target
  and run next are the same node."
  [graph target-node-id run-next]
  (let [{target-run-next ::run-next} (get-node graph target-node-id)
        graph (if target-run-next
                (remove-after-node graph target-run-next target-node-id)
                graph)]
    (cond
      (not run-next)
      (set-node-run-next* graph target-node-id run-next)

      (and run-next (not= target-node-id run-next))
      (-> graph
          (set-node-run-next* target-node-id run-next)
          (add-after-node run-next target-node-id))

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

(defn remove-branch-node-after-nodes
  "When node-id is a branch node, remove all after-nodes associated from its children."
  [graph node-id]
  (let [node (get-node graph node-id)]
    (if-let [branches (node-branches node)]
      (reduce
        (fn [g n-id]
          (remove-after-node g n-id node-id))
        graph
        branches)
      graph)))

(defn remove-node
  "Remove a node from the graph. In case of resolver nodes it also removes them
  from the ::index-syms and after node references."
  [graph node-id]
  (let [{::keys [run-next node-parents] :as node} (get-node graph node-id)]
    (assert (if node-parents
              (every? #(not= node-id (-> (get-node graph %) ::run-next))
                node-parents)
              true)
      "Tried to remove node that still contains references pointing to it. Move
      the run-next references from the pointer nodes before removing it.")
    (-> graph
        (cond->
          (pc-sym node)
          (update-in [::index-resolver->nodes (pc-sym node)] disj node-id))
        (remove-branch-node-after-nodes node-id)
        (remove-after-node run-next node-id)
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

(defn merge-nodes-run-next
  "Updates target-node-id run-next with the run-next of the last argument. This will do an AND
  branch operation with node-id run-next and run-next, updating the reference of node-id
  run-next."
  [{::keys [root] :as graph} env target-node-id {::keys [run-next]}]
  (let [merge-into-node (get-node graph target-node-id)
        run-next-node   (get-node graph run-next)]
    (if (same-resolver? merge-into-node run-next-node)
      (collapse-dynamic-nodes graph env target-node-id run-next)
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

(defn transfer-node-after-nodes
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
          (transfer-node-after-nodes target-node-id node)
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
          (transfer-node-after-nodes target-node-id node)
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
          (transfer-node-after-nodes target-node-id node)
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
        (= branch-type ::run-and)
        (merge-node-expects root (get-node graph node-id)))

      (= (::run-next root-node) node-id)
      graph

      :else
      (if (and (= branch-type ::run-and)
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
            (add-after-node node-id root)
            (cond->
              (= branch-type ::run-and)
              (merge-node-expects root node)

              (optimize-merge? graph node)
              (update-in [::nodes node-id] dissoc ::run-next)))))))

(defn create-branch-node
  [{::keys [root] :as graph} env node branch-node]
  (let [root-next      (get-in graph [::nodes root ::run-next])
        optimize-next? (optimize-merge? graph node)
        branch-node    (cond-> branch-node
                         optimize-next?
                         (assoc ::run-next root-next))
        branch-node-id (::node-id branch-node)]
    (-> graph
        (assoc-in [::nodes branch-node-id] branch-node)
        (add-after-node root branch-node-id)
        (cond-> optimize-next?
          (-> (update-in [::nodes root] dissoc ::run-next)
              (set-after-node root-next branch-node-id)))
        (set-root-node branch-node-id)
        (add-branch-node env node))))

(defn branch-add-and-node [graph branch-node-id node-id]
  (-> (update-in graph [::nodes branch-node-id ::run-and] coll/sconj node-id)
      (add-after-node node-id branch-node-id)))

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
          (transfer-node-after-nodes target-node-id node)
          (remove-node node-id))
      graph)))

(defn compute-root-branch
  [{::keys [root] :as graph}
   {::keys [branch-type] :as env}
   {::keys [node-id]}
   branch-node-factory]
  (if node-id
    (let [root-node (get-root-node graph)
          next-node (get-node graph node-id)
          root-sym  (pc-sym root-node)
          next-sym  (pc-sym next-node)]
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
        (-> (collapse-nodes-branch graph env node-id root)
            (set-root-node node-id)
            (add-snapshot! env {::snapshot-message (str "Collapsed nodes " node-id " and " root " due to same resolver call.")}))

        ; merge ands
        (and (::run-and root-node)
             (::run-and next-node)
             (can-merge-and-nodes? root-node next-node))
        (-> (collapse-and-nodes graph node-id root)
            (set-root-node node-id)
            (add-snapshot! env {::snapshot-message (str "Merged AND nodes " node-id " with " root)}))

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

        :else
        (-> (create-branch-node graph env next-node (branch-node-factory))
            (add-snapshot! env {::snapshot-message (str "Create new branch node for " node-id " and " root)}))))
    graph))

(defn compute-root-or
  [{::keys [root] :as graph}
   {::p.attr/keys [attribute] :as env}
   {::keys [node-id] :as node}]
  (if (= root node-id)
    graph
    (compute-root-branch graph (assoc env ::branch-type ::run-or) node
      (fn []
        {::node-id (next-node-id env)
         ::expects {attribute {}}
         ::run-or  #{(::root graph)}}))))

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
                (update-in oir [attr #{}] coll/sconj dynamic-base-provider-sym))
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
                                                      ::pci/index-oir]))
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
        final-deps     (reduce coll/merge-grow (fsd/ast->shape-descriptor ast) nodes-inputs)
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
                            ::snapshot-message (str "Included node " sym)}))))

(>defn direct-node-successors
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

(defn collect-syms
  ([graph env node] (collect-syms graph env node #{}))
  ([graph
    env
    {::keys [node-id]} syms]
   (let [node (get-in graph [::nodes node-id])]
     (if-let [sym (pc-sym node)]
       (if (dynamic-resolver? env sym)
         syms
         (conj syms sym))
       (into syms (mapcat #(collect-syms graph env {::node-id %}) (direct-node-successors node)))))))

(defn all-attribute-resolvers
  [{::pci/keys [index-oir]}
   attr]
  (if-let [ir (get index-oir attr)]
    (into #{} cat (vals ir))
    #{}))

(defn mark-node-unreachable
  [previous-graph
   graph
   {::keys [unreachable-resolvers
            unreachable-attrs]}
   env]
  (let [syms (->> (collect-syms graph env (get-root-node graph))
                  (into unreachable-resolvers)
                  (into (::unreachable-resolvers previous-graph)))]
    (add-snapshot! graph env {::snapshot-message (str "Mark node unreachable, resolvers " (pr-str syms) ", attrs" unreachable-attrs)})
    (cond-> (assoc previous-graph
              ::unreachable-resolvers syms
              ::unreachable-attrs unreachable-attrs)
      (set/subset? (all-attribute-resolvers env (pc-attr env)) syms)
      (update ::unreachable-attrs conj (pc-attr env)))))

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
  (let [successors (direct-node-successors (get-node graph node-id))]
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

(>defn first-common-ancestor
  "Find first common node ancestor given a list of node ids."
  [graph node-ids]
  [::graph ::node-id-set => ::node-id]
  (if (= 1 (count node-ids))
    (first node-ids)
    (let [ancestors  (mapv #(node-ancestors graph %) node-ids)
          ancestors' (into #{} (mapcat #(next %)) ancestors)
          nodes'     (into #{} (remove ancestors') node-ids)]
      (if (not= (count node-ids) (count nodes'))
        (first-common-ancestor graph nodes')
        (->> (reduce
               (fn [node-chain new-chain]
                 (let [chain-set (set node-chain)]
                   (filter chain-set new-chain)))
               ancestors)
             first)))))

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

(defn compute-missing-chain
  "Start a recursive call to process the dependencies required by the resolver. It
  sets the ::run-next data at the env, it will be used to link the nodes after they
  are created in the process."
  [graph {::keys [graph-before-missing-chain] :as env} missing]
  (comment
    [::graph (s/keys :req [::graph-before-missing-chain]) (s/coll-of ::eql/property :kind set?)
     => ::graph])
  (if (seq missing)
    (let [_             (add-snapshot! graph env {::snapshot-message (str "Computing " (pr-str missing) " dependencies for " (pc-attr env))})
          {::keys [index-attrs] :as graph'}
          (compute-run-graph*
            (dissoc graph ::root)
            (-> env
                (dissoc pc-attr)
                (update ::run-next-trail coll/sconj (::root graph))
                (update ::attr-deps-trail coll/sconj (pc-attr env))
                (assoc :edn-query-language.ast/node (eql/query->ast (vec missing)))))
          still-missing (remove (or index-attrs {}) missing)
          all-provided? (not (seq still-missing))]
      (if all-provided?
        (let [ancestor (find-missing-ancestor graph' missing)]
          (assert ancestor "Error finding ancestor during missing chain computation")
          (cond-> (merge-nodes-run-next graph' env ancestor {::run-next (::root graph)})
            (::run-and (get-root-node graph'))
            (merge-node-expects (::root graph') {::expects (zipmap missing (repeat {}))})

            true
            (add-snapshot! env {::snapshot-message (str "Chaining dependencies for " (pc-attr env) ", set node " (::root graph) " to run after node " ancestor)})))
        (let [{::keys [unreachable-resolvers] :as out'} (mark-node-unreachable graph-before-missing-chain graph graph' env)
              unreachable-attrs (filter #(set/subset? (all-attribute-resolvers env %) unreachable-resolvers) still-missing)]
          (update out' ::unreachable-attrs into unreachable-attrs))))
    graph))

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
   inputs resolvers]
  (let [missing (into #{} (remove #(contains? available-data %)) inputs)
        env     (assoc env ::input (into {} (map #(hash-map % {})) inputs))]
    (if (contains? inputs (pc-attr env))
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
              (compute-missing-chain (assoc env ::graph-before-missing-chain graph) missing)
              (compute-root-or env {::node-id (::root graph)}))
          (set-root-node <> (::root graph)))))))

(defn node-for-attribute-in-chain
  "Walks the graph run next chain until it finds the node that's providing the
  attribute."
  [graph
   {::p.attr/keys [attribute] :as env}
   root]
  (loop [node-id root]
    (let [{::keys [run-next expects run-and]} (get-node graph node-id)]
      (cond
        run-and
        (some #(node-for-attribute-in-chain graph env %)
          (cond->> run-and
            run-next
            (into [run-next])))

        (contains? expects attribute)
        node-id

        run-next
        (recur run-next)))))

(defn set-node-source-for-attrs
  [graph {::p.attr/keys [attribute] :as env}]
  (if-let [node-id (node-for-attribute-in-chain graph env (::root graph))]
    (-> graph
        (update-in [::nodes node-id ::source-for-attrs] coll/sconj attribute)
        (update ::index-attrs assoc attribute (get-in graph [::index-attrs attribute] node-id)))
    graph))

(defn node-direct-ancestor-chain
  "Computes ancestor chain for a given node, it only walks as long as there is a single
  parent on the node, if there is a fork (multiple after-nodes) it will stop."
  [graph node-id]
  (loop [node-id' node-id
         visited  #{}
         chain    (list)]
    (if (contains? visited node-id')
      (do
        (println "Ancestors Cycle detected" visited node-id')
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
  "Traverse node after-node chain and returns the most distant resolver ancestor of node id.
  This only walks though resolver nodes, branch nodes are removed."
  [graph node-id]
  (->> (node-direct-ancestor-chain graph node-id)
       (remove (comp ::run-and #(get-node graph %)))
       (remove (comp ::run-or #(get-node graph %)))
       first))

(defn push-root-to-ancestor [graph node-id]
  (set-root-node graph (find-furthest-ancestor graph node-id)))

(defn compute-and-unless-root-is-ancestor [{::keys [root] :as graph} env {::keys [node-id]}]
  (if (= node-id (first (node-direct-ancestor-chain graph root)))
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
                              ::snapshot-message (str "Associating previous root to attribute " attribute " on node " node-id)})
          (merge-node-expects node-id {attribute {}})
          (push-root-to-ancestor node-id)
          (compute-and-unless-root-is-ancestor env {::node-id root})
          (add-snapshot! env {::snapshot-event   ::snapshot-reuse-attribute-merged
                              ::snapshot-message (str "Merged attribute " attribute " on node " node-id " with root " root)})))

    :else
    (let [graph'
          (as-> graph <>
            (add-snapshot! <> env {::snapshot-event   ::snapshot-process-attribute
                                   ::snapshot-message (str "Process attribute " attribute)})
            (dissoc <> ::root)
            (reduce-kv
              (fn [graph inputs resolvers]
                (compute-input-resolvers-graph graph env inputs resolvers))
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
  [graph {:keys [key children]}]
  (if children
    (update graph ::nested-available-process coll/sconj key)
    graph))

(defn add-ident-process [graph {:keys [key]}]
  (update graph ::idents coll/sconj key))

(defn compute-attribute-graph
  "Compute the run graph for a given attribute."
  [{::keys [unreachable-attrs] :as graph}
   {::keys     [available-data attr-deps-trail]
    ::pci/keys [index-oir]
    {attr :key
     :as  ast} :edn-query-language.ast/node
    :as        env}]
  (let [env (assoc env pc-attr attr)]
    (cond
      (eql/ident? attr)
      (-> (add-ident-process graph ast)
          (add-snapshot! env {::snapshot-event   ::snapshot-add-ident-proces
                              ::snapshot-message (str "Add ident process for " (pr-str attr))}))

      (contains? available-data attr)
      (-> (mark-attribute-process-sub-query graph ast)
          (add-snapshot! env {::snapshot-event   ::snapshot-mark-process-sub-query
                              ::snapshot-message (str "Mark attribute " attr " to sub-process.")}))

      (or (contains? unreachable-attrs attr)
          (contains? attr-deps-trail attr))
      graph

      (contains? index-oir attr)
      (compute-attribute-graph* graph env)

      :else
      (-> (add-unreachable-attr graph attr)
          (add-snapshot! env {::snapshot-event   ::snapshot-mark-attr-unreachable
                              ::snapshot-message (str "Mark attribute " attr " as unreachable.")})))))

(defn compute-run-graph*
  [graph env]
  (reduce
    (fn [graph ast]
      (if (contains? #{:prop :join} (:type ast))
        (compute-attribute-graph graph
          (assoc env :edn-query-language.ast/node ast))
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
                                   ::after-nodes      #{3}}
                                2 {::pco/op-name          b
                                   ::node-id          2
                                   ::requires         {:b {}}
                                   ::input            {}
                                   ::source-for-attrs #{:b}
                                   ::after-nodes      #{3}}
                                3 {::node-id  3
                                   ::requires {:b {} :a {} :c {}}
                                   ::run-and  #{2 1 4}}
                                4 {::pco/op-name          c
                                   ::node-id          4
                                   ::requires         {:c {}}
                                   ::input            {}
                                   ::source-for-attrs #{:c}
                                   ::after-nodes      #{3}}}
       ::index-resolver->nodes {a #{1} b #{2} c #{4}}
       ::unreachable-resolvers #{}
       ::unreachable-attrs     #{}
       ::index-attrs           {:a 1 :b 2 :c 4}
       ::root                  3}
  "
  ([env]
   [(s/keys
      :req [:edn-query-language.ast/node
            ::pci/index-oir]
      :opt [::available-data
            ::pci/index-resolvers])
    => ::graph]
   (compute-run-graph {} env))

  ([graph env]
   [(? (s/keys))
    (s/keys
      :req [:edn-query-language.ast/node
            ::pci/index-oir]
      :opt [::available-data
            ::plan-cache*
            ::pci/index-resolvers])
    => ::graph]
   (add-snapshot! graph env {::snapshot-event   ::snapshot-start-graph
                             ::snapshot-message "Start query plan"})
   (p.cache/cached ::plan-cache* env [(::available-data env)
                                      (:edn-query-language.ast/node env)]
     #(compute-run-graph*
        (merge (base-graph)
               graph
               {::index-ast (pf.eql/index-ast (:edn-query-language.ast/node env))})
        (merge (base-env) env)))))

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
  ([env cache*] [map? refs/atom? => map?] (assoc env ::plan-cache* cache*)))
