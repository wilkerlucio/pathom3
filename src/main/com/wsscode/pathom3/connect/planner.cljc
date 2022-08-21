(ns com.wsscode.pathom3.connect.planner
  (:require
    [clojure.set :as set]
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [com.fulcrologic.guardrails.core :refer [>def >defn >fdef => | <- ?]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.misc.refs :as refs]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.cache :as p.cache]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.path :as p.path]
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
  (? ::pfsd/shape-descriptor))

(>def ::user-request-shape
  "An shape descriptor declaring which data is required to fulfill the user request."
  (? ::pfsd/shape-descriptor))

(>def ::node-parents
  "A set of node-ids containing the direct parents of the current node.
  In regular execution nodes, this is the reverse of ::run-next, but in case of
  immediate children of branch nodes, this points to the branch node."
  ::node-id-set)

(>def ::attr-deps-trail
  "A set containing attributes already in consideration when computing missing dependencies."
  ::p.attr/attributes-set)

(>def ::attr-resolvers-trail
  "A set containing resolvers already in consideration for the plan, used to track cycles
  on nested inputs"
  (s/coll-of ::pco/op-name :kind set?))

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

(>def ::node-resolution-checkpoint?
  "Mark to Pathom that it should verify graph completion after running this done"
  boolean?)

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
  "On dynamic resolvers, this points to the original source resolver in the foreign environment."
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
  "A vector with the operation name of every mutation that appears in the query."
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

(>def ::optimize-graph? boolean?)

(>def ::plan-cache*
  "Atom containing the cache atom to support cached planning."
  p.cache/cache-store?)

(>def ::snapshots*
  "Atom to store each step of the planning process"
  refs/atom?)

(>def ::denorm-update-node
  "Function to update denormalized node before indexing it. Useful for post processing,
  for example to keep only a subset of the node data, to enable direct comparison."
  fn?)

; endregion

(declare add-snapshot! compute-run-graph compute-run-graph* compute-attribute-graph
         optimize-graph optimize-node remove-node-expects-index-attrs find-leaf-node)

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
  [graph env node-id]
  (let [node      (get-node graph node-id)
        node-name (or (::source-op-name node)
                      (::pco/op-name node))]
    (if-let [config (some->> node-name (pci/resolver-config env))]
      (merge node config))))

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
     (update-in graph (cond-> [::nodes node-id] k (conj k)) f)
     graph))
  ([graph node-id k f v]
   (if (get-node graph node-id)
     (update-in graph (cond-> [::nodes node-id] k (conj k)) f v)
     graph))
  ([graph node-id k f v v2]
   (if (get-node graph node-id)
     (update-in graph (cond-> [::nodes node-id] k (conj k)) f v v2)
     graph))
  ([graph node-id k f v v2 v3]
   (if (get-node graph node-id)
     (update-in graph (cond-> [::nodes node-id] k (conj k)) f v v2 v3)
     graph))
  ([graph node-id k f v v2 v3 & args]
   (if (get-node graph node-id)
     (apply update-in graph (cond-> [::nodes node-id] k (conj k)) f v v2 v3 args)
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

(defn node-branch-type
  [node]
  (cond
    (::run-and node) ::run-and
    (::run-or node) ::run-or))

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

(defn attr-optional? [{::keys [index-ast]} attribute-kw]
  (let [attribute (get index-ast attribute-kw)]
    ;; If the attribute isn't in the index-ast, then it can be considered optional
    (or (nil? attribute)
        (get-in attribute [:params ::pco/optional?]))))

(defn node-optional? [{::keys [params]}]
  (::pco/optional? params))

(defn add-node-parent [graph node-id node-parent-id]
  (assert node-parent-id "Tried to add after node with nil value")
  (update-node graph node-id ::node-parents coll/sconj node-parent-id))

(defn remove-node-parent
  "Disconnect the parent node node-parent-id from node node-id"
  [graph node-id node-parent-id]
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
  "Set the node run next value and add the node-parent counterpart. Noop if target
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

(defn set-node-expects
  "Set node expects, this also removes previous references from index-attrs and add
  new ones for the new expects."
  [graph node-id expects]
  (-> graph
      (remove-node-expects-index-attrs node-id)
      (assoc-node node-id ::expects expects)
      (as-> <>
        (reduce
          (fn [g attr]
            (update-in g [::index-attrs attr] coll/sconj node-id))
          <>
          (keys expects)))))

(defn set-node-source-for-attrs
  ([graph env] (set-node-source-for-attrs graph env (::root graph)))
  ([graph {::p.attr/keys [attribute]} node-id]
   (if node-id
     (-> graph
         (update-in [::index-attrs attribute] coll/sconj node-id))
     graph)))

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

(defn remove-from-parent-branches
  "Disconnect a branch node from its parents."
  [graph {::keys [node-id node-parents]}]
  (reduce
    (fn [g nid]
      (let [n (get-node graph nid)]
        (cond
          (contains? (::run-and n) node-id)
          (-> g
              (update-node nid ::run-and disj node-id)
              (remove-node-parent node-id nid))

          (contains? (::run-or n) node-id)
          (-> g
              (update-node nid ::run-or disj node-id)
              (remove-node-parent node-id nid))

          (= (::run-next n) node-id)
          (-> g
              (set-node-run-next nid nil)
              (remove-node-parent node-id nid))

          :else
          g)))
    graph
    node-parents))

(defn remove-run-next-edge [graph node-id]
  (let [{::keys [run-next]} (get-node graph node-id)]
    (if run-next
      (-> graph
          (remove-node-parent run-next node-id)
          (set-node-run-next node-id nil))
      graph)))

(defn remove-node-edges
  "Remove all node connections. This disconnect the nodes from parents and run-next."
  [graph node-id]
  (let [node (get-node graph node-id)]
    (-> graph
        (remove-from-parent-branches node)
        (remove-run-next-edge node-id))))

(defn move-branch-item-node
  "Move a branch item from source parent target parent."
  [graph target-parent-id node-id]
  (let [node        (get-node graph node-id)
        branch-type (-> (get-node graph target-parent-id)
                        (node-branch-type))]
    (-> (remove-from-parent-branches graph node)
        (add-branch-to-node target-parent-id branch-type node-id))))

(defn disj-rem [m k item]
  (let [new-val (disj (get m k) item)]
    (if (seq new-val)
      (assoc m k new-val)
      (dissoc m k))))

(defn remove-node-expects-index-attrs
  "Since the node has attribute indexes associated with it, this removes those links
  considering the attributes listed on expects."
  [graph node-id]
  (let [expects (get-node graph node-id ::expects)]
    (reduce
      (fn [g attr]
        (update g ::index-attrs disj-rem attr node-id))
      graph
      (keys expects))))

(defn remove-node*
  "Remove a node from the graph. Doesn't remove any references, caution!"
  [graph node-id]
  (let [node    (get-node graph node-id)
        op-name (::pco/op-name node)]
    (-> graph
        (cond->
          op-name
          (update ::index-resolver->nodes
            (fn [idx]
              (let [next (disj (get idx op-name) node-id)]
                (if (seq next)
                  (assoc idx op-name next)
                  (dissoc idx op-name))))))
        (update ::nodes dissoc node-id))))

(defn remove-node
  "Remove a node from the graph. In case of resolver nodes it also removes them
  from the ::index-syms, the index-attrs and after node references."
  [graph node-id]
  (let [{::keys [run-next node-parents] :as node} (get-node graph node-id)]
    (assert (if node-parents
              (every? #(not= node-id (get-node graph % ::run-next))
                node-parents)
              true)
      (str "Tried to remove node " node-id " that still contains references pointing to it. Move
      the run-next references from the pointer nodes before removing it. Also check if
      parent is branch and trying to merge."))
    (-> graph
        (remove-branch-node-parents node-id)
        (remove-node-parent run-next node-id)
        (remove-from-parent-branches node)
        (remove-node-expects-index-attrs node-id)
        (remove-node* node-id))))

(defn find-chain-start
  "Given a graph and a node-id, walks the run-next chain until it finds the first node
  that's connected to a branch parent (via branch relationship, not as run next) or
  the root."
  [graph node-id]
  (let [{::keys [node-parents]} (get-node graph node-id)
        parent-id (first node-parents)]
    (if parent-id
      (let [parent (get-node graph parent-id)]
        (if (= (::run-next parent) node-id)
          (recur graph parent-id)
          node-id))
      node-id)))

(defn remove-root-node-cluster
  "Remove a complete node cluster, starting from some node root."
  [graph node-ids]
  (if (seq node-ids)
    (let [[node-id & rest] node-ids
          node-id'   (find-chain-start graph node-id)
          {::keys [run-next] :as node} (get-node graph node-id')
          branches   (or (node-branches node) #{})
          next-nodes (cond-> branches run-next (conj run-next))]
      (recur (remove-node graph node-id')
        (into rest next-nodes)))
    graph))

(defn include-node
  "Add new node to the graph, this add the node and the index of in ::index-syms."
  [graph {::keys [node-id] ::pco/keys [op-name] :as node}]
  (-> graph
      (assoc-in [::nodes node-id] node)
      (cond->
        op-name
        (update-in [::index-resolver->nodes op-name] coll/sconj node-id))))

(defn create-and [graph env node-ids]
  (if (= 1 (count node-ids))
    (get-node graph (first node-ids))
    (let [{and-node-id ::node-id
           :as         and-node} (new-node env {})]
      (-> graph
          (include-node and-node)
          (add-node-branches and-node-id ::run-and node-ids)))))

(defn create-root-and [graph env node-ids]
  (if (= 1 (count node-ids))
    (set-root-node graph (first node-ids))
    (let [{and-node-id ::node-id
           :as         and-node} (new-node env {})]
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

(defn node-attribute-provides
  "For a specific attribute, return a vector containing the provides of each node of
  that resolver, or the current available data for it."
  [graph env attr]
  (if-let [available (get-in graph [::available-data attr])]
    [available]
    (some->>
      (get-in graph [::index-attrs attr])
      (mapv
        #(-> (node-with-resolver-config graph env %)
             ::pco/provides
             (get attr))))))

(defn transfer-node-parent
  "Transfer the node parent from source node to target node. This function will also
  update the parents references to point to target node."
  [graph target-node-id source-node-id node-id]
  (-> graph
      (remove-node-parent source-node-id node-id)
      (add-node-parent target-node-id node-id)
      (as-> <>
        (cond
          (= (get-node graph node-id ::run-next) source-node-id)
          (set-node-run-next* <> node-id target-node-id)

          (contains? (get-node graph node-id ::run-and) source-node-id)
          (-> <>
              (add-branch-to-node node-id ::run-and target-node-id)
              (update-node node-id ::run-and disj source-node-id))

          (contains? (get-node graph node-id ::run-or) source-node-id)
          (-> <>
              (add-branch-to-node node-id ::run-or target-node-id)
              (update-node node-id ::run-or disj source-node-id))

          :else
          <>))))

(defn transfer-node-parents
  "Transfer node parents from source node to target node. In case source node is root,
  the root will be transferred to target node."
  [graph target-node-id source-node-id]
  (let [parents (get-node graph source-node-id ::node-parents)]
    (-> graph
        ; transfer root
        (cond->
          (= (::root graph) source-node-id)
          (set-root-node target-node-id))
        (as-> <>
          (reduce
            (fn [g node-id]
              (transfer-node-parent g target-node-id source-node-id node-id))
            <>
            parents)))))

(declare denormalize-node)

(defn get-denormalized-node [graph node-id]
  (get-in graph [::nodes-denormalized node-id]))

(defn denormalize-node
  "Compute a version of the node that contains all forward references denormalized. It means
  instead of having the ids at run-next/branches (both OR and AND) the node will have
  the node data itself directly there. The denormalized version is added using the node-id
  at the ::nodes-denormalized key in the graph. All subsequent nodes are also denormalized
  in the process and also add to ::nodes-denormalized, so a lookup for then will be
  also readly available."
  [{::keys [denorm-update-node] :as graph} node-id]
  (if (get-denormalized-node graph node-id)
    graph
    (let [{::keys [run-next run-and run-or] :as node} (get-node graph node-id)
          branches (node-branches node)
          graph'   (cond-> graph
                     run-next (denormalize-node run-next)
                     branches (as-> <> (reduce denormalize-node <> branches)))

          node'    (cond-> node
                     run-next (assoc ::run-next (get-denormalized-node graph' run-next))
                     run-and (assoc ::run-and (into #{} (map #(get-denormalized-node graph' %)) branches))
                     run-or (assoc ::run-or (into #{} (map #(get-denormalized-node graph' %)) branches))

                     denorm-update-node denorm-update-node)]
      (assoc-in graph' [::nodes-denormalized node-id] node'))))

(defn combine-expects [na nb]
  (update na ::expects pfsd/merge-shapes (::expects nb)))

(defn combine-inputs [na nb]
  (if (::input nb)
    (update na ::input pfsd/merge-shapes (::input nb))
    na))

(defn combine-foreign-ast [na nb]
  (if (::foreign-ast nb)
    (update na ::foreign-ast pf.eql/merge-ast-children (::foreign-ast nb))
    na))

(defn transfer-node-indexes [graph target-node-id source-node-id]
  (let [attrs (keys (get-node graph source-node-id ::expects))]
    (reduce
      (fn [graph attr]
        (-> graph
            (update-in [::index-attrs attr] coll/sconj target-node-id)
            (update-in [::index-attrs attr] disj source-node-id)))
      graph
      attrs)))

(defn combine-run-next
  [graph env node-ids pivot]
  (let [run-next-nodes (into []
                             (comp (map #(get-node graph %))
                                   (filter ::run-next))
                             node-ids)]
    (cond
      (= 1 (count run-next-nodes))
      (let [{::keys [node-id run-next]} (first run-next-nodes)]
        (-> graph
            (remove-node-parent run-next node-id)
            (set-node-run-next pivot run-next)))

      (seq run-next-nodes)
      (let [and-node (new-node env {::run-and #{}})]
        (as-> graph <>
          (include-node <> and-node)
          (reduce
            (fn [g {::keys [node-id run-next]}]
              (-> g
                  (remove-node-parent run-next node-id)
                  (add-branch-to-node (::node-id and-node) ::run-and run-next)))
            <>
            run-next-nodes)
          (set-node-run-next <> pivot (::node-id and-node))))

      :else
      graph)))

(defn move-run-next-to-edge
  "Find the node at the end of chain from target-node-id and move node-to-move-id
  as the next of that"
  [graph target-node-id node-to-move-id]
  (let [leaf (find-leaf-node graph (get-node graph target-node-id))]
    (-> graph
        (remove-from-parent-branches (get-node graph node-to-move-id))
        (set-node-run-next (::node-id leaf) node-to-move-id))))

(>defn simplify-branch-node
  "When a branch node contains a single branch out, remove the node and put that
  single item in place.

  Note in case the branch has a run-next, that run-next gets moved to the end of chain
  to retain the same order as it would run with the branch."
  [graph env node-id]
  [::graph map? ::node-id => ::graph]
  (let [node           (get-node graph node-id)
        target-node-id (or
                         (and
                           (= 1 (count (::run-and node)))
                           (first (::run-and node)))
                         (and
                           (= 1 (count (::run-or node)))
                           (first (::run-or node))))]
    (if target-node-id
      (-> graph
          (add-snapshot! env {::snapshot-message "Simplifying branch with single element"
                              ::highlight-nodes  #{node-id target-node-id}
                              ::highlight-styles {node-id 1}})
          (cond->
            (::run-next node)
            (move-run-next-to-edge target-node-id (::run-next node)))
          (transfer-node-parents target-node-id node-id)
          (update-node target-node-id nil combine-expects node)
          (remove-node-edges node-id)
          (remove-node node-id)
          (add-snapshot! env {::snapshot-message "Simplification done"
                              ::highlight-nodes  #{target-node-id}})
          (optimize-node env target-node-id))
      graph)))

(defn merge-sibling-resolver-node
  "Merges data from source-node-id into target-node-id, them removes the source node."
  [graph target-node-id source-node-id]
  (let [source-node (get-node graph source-node-id)]
    (-> graph
        ; merge any extra keys from source node, but without overriding anything
        (update-node target-node-id nil coll/merge-defaults source-node)
        (update-node target-node-id nil combine-expects source-node)
        (update-node target-node-id nil combine-inputs source-node)
        (update-node target-node-id nil combine-foreign-ast source-node)
        (update-node target-node-id nil dissoc ::source-op-name)
        (transfer-node-indexes target-node-id source-node-id)
        (remove-node-edges source-node-id)
        (remove-node source-node-id))))

(defn merge-sibling-resolver-nodes*
  [graph pivot node-ids]
  (reduce
    (fn [g node-id]
      (merge-sibling-resolver-node g pivot node-id))
    graph
    node-ids))

(defn merge-sibling-resolver-nodes
  [graph env parent-node-id node-ids]
  (let [[pivot & node-ids'] node-ids
        resolver (::pco/op-name (get-node graph pivot))]
    (add-snapshot! graph env {::snapshot-message (str "Merging sibling resolver calls to resolver " resolver)
                              ::highlight-nodes  (into #{} (conj node-ids parent-node-id))
                              ::highlight-styles {parent-node-id 1}})
    (-> graph
        (combine-run-next env node-ids pivot)
        (merge-sibling-resolver-nodes* pivot node-ids')
        (add-snapshot! env {::snapshot-message "Merge complete"
                            ::highlight-nodes  #{parent-node-id pivot}
                            ::highlight-styles {parent-node-id 1}}))))

; endregion

; region graph helpers

(defn add-snapshot!
  ([graph {::keys [snapshots* snapshot-depth]} event-details]
   (if snapshots*
     (let [pad            (str/join (repeat (or snapshot-depth 0) "-"))
           pad            (if (seq pad) (str pad " ") "")
           event-details' (coll/update-if event-details ::snapshot-message #(str pad %))]
       (swap! snapshots* conj (-> graph (dissoc ::source-ast ::available-data)
                                  (merge event-details')))))
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
  (or (some-> env meta ::original-env
              (with-meta (meta env)))
      env))

(defn push-path
  [env {::p.attr/keys [attribute]
        ::p.path/keys [path]}]
  (cond-> env
    attribute
    (assoc ::p.path/path (coll/vconj path attribute))))

(defn add-unreachable-path
  "Add attribute to unreachable list"
  [graph env path]
  (-> (update graph ::unreachable-paths pfsd/merge-shapes path)
      (add-snapshot! env {::snapshot-event   ::snapshot-mark-attr-unreachable
                          ::snapshot-message (str "Mark path " (pr-str path) " as unreachable.")})))

(defn add-warning [graph warn]
  (update graph ::warnings coll/vconj warn))

(defn merge-unreachable
  "Copy unreachable attributes from discard-graph to target-graph. Using the extra arity
  you can also add a new unreachable path in the same call."
  ([target-graph {::keys [unreachable-paths]}]
   (cond-> target-graph
     unreachable-paths
     (update ::unreachable-paths pfsd/merge-shapes (or unreachable-paths {}))))
  ([target-graph graph env path]
   (-> (merge-unreachable target-graph graph)
       (add-unreachable-path env path))))

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

(defn inc-snapshot-depth [env]
  (update env ::snapshot-depth #(inc (or % 0))))

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

(>defn find-root-resolver-nodes
  "Returns the first resolvers to get called in the graph. This will traverse AND and OR
  node branches until the first resolvers are found. This function doesn't work the
  run-next of nodes."
  [{::keys [root] :as graph}]
  [::graph => ::node-id-set]
  (loop [nodes (transient #{})
         queue (coll/queue [root])]
    (if-let [node-id (peek queue)]
      (let [{::pco/keys [op-name] :as node} (get-node graph node-id)]
        (if op-name
          (recur (conj! nodes node-id) (pop queue))
          (recur nodes (into (pop queue) (node-branches node)))))
      (persistent! nodes))))

; endregion

; region sub-query process

(>defn shape-descriptor->ast-children-optional
  "Convert pathom output format into shape descriptor format."
  [shape]
  [::pfsd/shape-descriptor => vector?]
  (into []
        (map (fn [[k v]]
               (if (seq v)
                 {:type         :join
                  :key          k
                  :dispatch-key k
                  :children     (shape-descriptor->ast-children-optional v)
                  :params       {::pco/optional? true}}
                 {:type         :prop
                  :key          k
                  :dispatch-key k
                  :params       {::pco/optional? true}})))
        shape))

(>defn shape-descriptor->ast-optional
  "Convert pathom output format into shape descriptor format."
  [shape]
  [::pfsd/shape-descriptor => map?]
  {:type     :root
   :children (shape-descriptor->ast-children-optional shape)})

(defn extend-attribute-sub-query
  [graph {::keys [optional-process?]} attr shape]
  (-> graph
      (update-in [::index-ast attr] pf.eql/merge-ast-children
        (-> (if optional-process?
              (shape-descriptor->ast-optional shape)
              (pfsd/shape-descriptor->ast shape))
            (assoc :type :prop :key attr :dispatch-key attr)))))

(>defn shape-reachable?
  "Given an environment, available data and shape, determines if the whole shape
  is reachable (including nested dependencies)."
  [{::keys [resolvers attr-resolvers-trail] :as env} available shape]
  [map? ::pfsd/shape-descriptor ::pfsd/shape-descriptor => boolean?]
  (let [missing (pfsd/missing available shape)]
    (if (seq missing)
      (let [graph (compute-run-graph
                    (-> (reset-env env)
                        (push-path env)
                        (inc-snapshot-depth)
                        (assoc
                          ::optimize-graph? false
                          ::attr-resolvers-trail (into (or attr-resolvers-trail #{}) resolvers)
                          ::available-data available
                          :edn-query-language.ast/node (pfsd/shape-descriptor->ast missing))))]
        (every?
          (fn [[attr sub]]
            (if-let [nodes-subs (node-attribute-provides graph env attr)]
              (if (seq nodes-subs)
                (some #(shape-reachable? env % sub) nodes-subs)
                true)))
          shape))
      true)))

(defn compute-attribute-nested-input-require [graph env attr shape nodes]
  (add-snapshot! graph env {::snapshot-message (str "Processing nested requirements " (pr-str {attr shape}))
                            ::highlight-nodes  (into #{} (map ::node-id) nodes)})
  (let [checked-nodes (into []
                            (map (fn [node]
                                   (cond-> node
                                     (shape-reachable?
                                       (assoc env ::p.attr/attribute attr)
                                       (-> node ::pco/provides (get attr))
                                       shape)
                                     (assoc :valid-path? true))))
                            nodes)]
    (if (some :valid-path? checked-nodes)
      (-> (reduce
            (fn [g {:keys [valid-path?] ::keys [node-id]}]
              (if-not valid-path?
                (assoc-node g node-id ::invalid-node? true)
                g))
            graph
            checked-nodes)
          (extend-attribute-sub-query env attr shape))
      (-> graph
          (dissoc ::root)
          (add-unreachable-path env {attr shape})))))

(defn compute-attribute-dependency-graph
  [graph {::keys [recursive-joins] :as env} attr shape]
  (add-snapshot! graph env {::snapshot-message (str "Processing dependency " {attr shape})})
  (let [graph' (-> graph
                   (dissoc ::root)
                   (compute-attribute-graph
                     (-> env
                         (dissoc ::p.attr/attribute)
                         (update ::attr-deps-trail coll/sconj (::p.attr/attribute env))
                         (assoc
                           :edn-query-language.ast/node
                           (first (pfsd/shape-descriptor->ast-children {attr shape}))))))]
    (if (::root graph')
      (let [nodes
            (->> (get-in graph' [::index-attrs attr])
                 (mapv #(node-with-resolver-config graph' env %)))

            recur
            (get recursive-joins attr)]
        (cond
          ; recursive query
          recur
          (-> graph'
              (add-snapshot! env {::snapshot-message (str "Detected recursive nested dependency on " attr)})
              (update-in [::index-ast attr] assoc
                :type :join
                :key attr
                :dispatch-key attr
                :query recur))

          ; nested input requirement
          (seq shape)
          (compute-attribute-nested-input-require graph' env attr shape nodes)

          :else
          graph'))
      graph')))

(defn extend-available-attribute-nested
  [graph {::keys [available-data] :as env} attr shape]
  (if (shape-reachable? env (get available-data attr) shape)
    [(-> (extend-attribute-sub-query graph env attr shape)
         (mark-attribute-process-sub-query {:key attr :children []}))
     true]
    ; TODO maybe the sub-query fails partially, in this case making the whole
    ; sub-query unreachable will lead to bad results
    [(add-unreachable-path graph env {attr shape})
     false]))

; endregion

; region path expansion

(defn runner-node-sym
  "Find the runner symbol for a resolver, on normal resolvers that is the resolver symbol,
  but for foreign resolvers it uses its ::p.c.o/dynamic-name."
  [env resolver-name]
  (let [resolver (pci/resolver-config env resolver-name)]
    (or (::pco/dynamic-name resolver)
        resolver-name)))

(defn promote-foreign-ast-children
  "Moves the children from foreign ast to the main children. Also removes the foreign
  ast attribute."
  [{::keys [foreign-ast] :as ast}]
  (-> ast
      (dissoc ::foreign-ast)
      (cond->
        (:children foreign-ast)
        (assoc :children (:children foreign-ast)))))

(defn compute-dynamic-nested-requirements*
  [{ast :edn-query-language.ast/node
    :as env}
   dynamic-name
   available]
  (let [graph        (compute-run-graph (-> (reset-env env)
                                            (push-path env)
                                            (inc-snapshot-depth)
                                            (assoc
                                              :edn-query-language.ast/node ast
                                              ::available-data available)))
        root-res     (find-root-resolver-nodes graph)
        nested-needs (transduce
                       (map #(get-node graph %))
                       (completing
                         (fn [i {::keys [input expects] ::pco/keys [op-name]}]
                           (if (= op-name dynamic-name)
                             (pfsd/merge-shapes i expects)
                             (pfsd/merge-shapes i input))))
                       {}
                       root-res)
        ast-shape    (pfsd/ast->shape-descriptor ast)]
    (pfsd/merge-shapes nested-needs
                       (pfsd/intersection available ast-shape))))

(defn compute-dynamic-nested-union-requirements*
  [{ast :edn-query-language.ast/node
    :as env}
   dynamic-name
   available]
  (let [union-children (-> ast :children first :children)]
    (into ^::pfsd/union? {}
          (map (fn [{:keys [union-key] :as ast'}]
                 (coll/make-map-entry
                   union-key
                   (compute-dynamic-nested-requirements*
                     (assoc env :edn-query-language.ast/node ast')
                     dynamic-name
                     available))))
          union-children)))

(>defn compute-dynamic-nested-requirements
  "Considering the operation output, find out what a query can extend during nesting.

  Pathom uses it to compute the dynamic requirements to send into dynamic resolvers.

  This function is a useful tool for developers of custom dynamic resolvers."
  [{::p.attr/keys [attribute]
    ::pco/keys    [op-name]
    ast           :edn-query-language.ast/node
    :as           env}]
  [(s/keys :req [::pco/op-name
                 :edn-query-language.ast/node]
           :opt [::p.attr/attribute
                 ::p.path/path])
   => (? ::pfsd/shape-descriptor)]
  (if (seq (:children ast))
    (let [{::pco/keys [dynamic-name provides]} (pci/operation-config env op-name)
          dynamic-name (or dynamic-name (::pco/dynamic-name env))
          available    (if attribute
                         (get provides attribute)
                         provides)]
      (if (-> ast :children first :type (= :union))
        (compute-dynamic-nested-union-requirements* env dynamic-name available)
        (compute-dynamic-nested-requirements* env dynamic-name available)))))

(defn compute-nested-requirements
  [{::keys        [available-data]
    ::p.attr/keys [attribute]
    ::pco/keys    [op-name]
    ast           :edn-query-language.ast/node
    :as           env}]
  (let [config      (pci/resolver-config env op-name)
        provide-sub (get (::pco/provides config) attribute)
        graph       (compute-run-graph (-> (reset-env env)
                                           (push-path env)
                                           (inc-snapshot-depth)
                                           (assoc
                                             :com.wsscode.pathom3.error/lenient-mode? true
                                             :edn-query-language.ast/node ast
                                             ::available-data (pfsd/merge-shapes available-data provide-sub))))
        root-res    (find-root-resolver-nodes graph)
        root-inputs (transduce
                      (map #(get-node graph %))
                      (completing
                        (fn [i {::keys [input]}]
                          (pfsd/merge-shapes i input)))
                      {}
                      root-res)
        ast-shape   (pfsd/ast->shape-descriptor ast)]
    (pfsd/intersection (pfsd/merge-shapes ast-shape root-inputs)
                       provide-sub)))

(defn create-node-for-resolver-call
  "Create a new node representative to run a given resolver."
  [{::keys        [input]
    ::p.attr/keys [attribute]
    ::pco/keys    [op-name]
    ast           :edn-query-language.ast/node
    :as           env}]
  (let [ast-params  (:params ast)
        config      (pci/resolver-config env op-name)
        op-name'    (or (::pco/dynamic-name config) op-name)
        dynamic?    (pci/dynamic-resolver? env op-name')
        provide-sub (get (::pco/provides config) attribute)
        sub         (cond
                      dynamic?
                      (compute-dynamic-nested-requirements env)

                      (and (seq provide-sub) (seq (:children ast)))
                      (compute-nested-requirements env))
        requires    {attribute (cond-> (or sub {})
                                 (seq ast-params)
                                 (pfsd/shape-params ast-params))}]

    (cond->
      (new-node env
                {::pco/op-name op-name'
                 ::expects     requires
                 ::input       input})

      (seq ast-params)
      (assoc ::params ast-params)

      dynamic?
      (assoc
        ::source-op-name
        op-name

        ::foreign-ast
        {:type     :root
         :children (if sub
                     (let [ast' (pfsd/shape-descriptor->ast sub)]
                       [(assoc ast
                          :children (:children ast')
                          :query (pfsd/shape-descriptor->query sub))])
                     [ast])}))))

(defn compute-resolver-leaf
  "For a set of resolvers (the R part of OIR index), create one OR node that branches
  to each option in the set."
  [graph {::keys [input] :as env} resolvers]
  (let [resolver-nodes (into
                         (list)
                         (map #(create-node-for-resolver-call (assoc env ::pco/op-name %)))
                         resolvers)]
    (if (seq resolver-nodes)
      (-> (reduce #(-> %
                       (include-node %2)
                       (set-node-source-for-attrs env (::node-id %2))) graph resolver-nodes)
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
        (let [[graph' extended?] (extend-available-attribute-nested graph env attr shape)]
          (if extended?
            [graph' node-map]
            (reduced [graph' nil])))

        (let [graph' (compute-attribute-dependency-graph graph env attr shape)]
          (if-let [root (::root graph')]
            [graph'
             (assoc node-map attr root)]
            (reduced [(merge-unreachable graph graph') nil])))))
    [graph {}]
    missing))

(defn compute-missing-chain-optional-deps
  [graph {::keys [available-data] :as env} opt-missing node-map]
  (reduce-kv
    (fn [[graph node-map] attr shape]
      (let [env (assoc env ::optional-process? true)]
        (if (contains? available-data attr)
          (let [[graph'] (extend-available-attribute-nested graph env attr shape)]
            [graph' node-map])

          (let [graph' (compute-attribute-dependency-graph graph env attr shape)]
            (if-let [root (::root graph')]
              [(cond-> graph'
                 (contains? node-map attr)
                 (-> (remove-root-node-cluster [(get node-map attr)])
                     (add-snapshot! env {::snapshot-message "Optional computation overrode the required."})))
               (assoc node-map attr root)]
              [(merge-unreachable graph graph') node-map])))))
    [graph node-map]
    opt-missing))

(defn- index-recursive-joins
  [env resolvers]
  (into {}
        (comp (map #(pci/resolver-config env %))
              (mapcat ::pco/input)
              (keep (fn [x] (if (and (map? x) (pf.eql/recursive-query? (first (vals x))))
                              (first x)))))
        resolvers))

(defn compute-missing-chain
  "Start a recursive call to process the dependencies required by the resolver."
  [graph env missing missing-optionals]
  (let [_ (add-snapshot! graph env {::snapshot-message (str "Computing " (::p.attr/attribute env) " dependencies: " (pr-str missing))})
        [graph' node-map] (compute-missing-chain-deps graph env missing)]
    (if (some? node-map)
      ;; add new optional nodes (and maybe nested processes)
      (let [[graph'' node-map-opts] (compute-missing-chain-optional-deps graph' env missing-optionals node-map)
            all-nodes (vals (merge node-map node-map-opts))]
        (-> graph''
            (cond->
              (seq all-nodes)
              (create-root-and env (vals (merge node-map node-map-opts)))

              (empty? all-nodes)
              (set-root-node (::root graph)))
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
   {::keys        [available-data attr-resolvers-trail]
    ::p.attr/keys [attribute]
    :as           env}
   input resolvers]
  (cond
    (contains? input attribute)
    ; attribute requires itself, just stop
    graph

    ; nested cycle, stop
    (some #(contains? attr-resolvers-trail %) resolvers)
    (let [failed (set/intersection (or attr-resolvers-trail #{}) resolvers)]
      (println (str "WARN: Nested cycle detected for attribute " attribute " on one of these resolvers: " (pr-str failed)))
      (add-snapshot! graph env
                     {::snapshot-event   ::snapshot-nested-cycle-dependency
                      ::snapshot-message (str "Nested cycle detected for attribute " attribute " on one of these resolvers: " (pr-str failed))}))

    :else
    (let [missing      (pfsd/missing available-data input)
          missing-opts (resolvers-missing-optionals env resolvers)
          env          (assoc env ::input input)
          {leaf-root ::root :as graph'} (compute-resolver-leaf graph env resolvers)]
      (if leaf-root
        (if (seq (merge missing missing-opts))
          (let [graph-with-deps (compute-missing-chain
                                  graph'
                                  (-> env
                                      (assoc
                                        ::resolvers resolvers
                                        ::recursive-joins (index-recursive-joins env resolvers))
                                      (update ::snapshot-depth #(inc (or % 0))))
                                  missing
                                  missing-opts)]
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

        [graph' node-ids unreachable-graphs]
        (reduce-kv
          (fn [[graph nodes unreachable-graphs] input resolvers]
            (let [graph' (compute-input-resolvers-graph (dissoc graph ::root) env input resolvers)]
              (if (::root graph')
                [graph' (conj nodes (::root graph')) unreachable-graphs]
                [graph nodes (conj unreachable-graphs graph')])))
          [graph #{} #{}]
          (get index-oir attribute))]
    (if (seq node-ids)
      (create-root-or graph' env node-ids)
      (-> graph
          (add-unreachable-path env {attribute {}})
          (as-> <>
            (reduce
              merge-unreachable
              <>
              unreachable-graphs))))))

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
      (add-unreachable-path graph env {attr {}}))))

(defn sanitize-params [params]
  (when (seq params)
    params))

(defn merge-ast-children [a b]
  (let [idx (pf.eql/index-ast a)]
    (reduce
      (fn [ast child]
        (if-let [entry (get idx (:key child))]
          (let [p1 (sanitize-params (:params entry))
                p2 (sanitize-params (:params child))]
            (if (= p1 p2)
              (if (and (seq (:children entry)) (seq (:children child)))
                (update ast :children
                  (fn [children]
                    (mapv
                      (fn [c]
                        (if (= c entry)
                          (merge-ast-children c child)
                          c))
                      children)))
                ast)
              (throw (ex-info "Incompatible placeholder request"
                              {:source-node      entry
                               :conflicting-node child}))))
          (update ast :children conj child)))
      a
      (:children b))))

(defn merge-placeholder-ast [index-ast placeholder-ast]
  (merge-with merge-ast-children
              index-ast
              (coll/filter-vals (comp seq :children) (pf.eql/index-ast placeholder-ast))))

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
    (-> (add-placeholder-entry graph attr)
        (cond->
          (empty? (:params ast))
          (-> (compute-run-graph* env)
              (update ::index-ast merge-placeholder-ast ast))))))

(defn plan-mutation-nested-query [env {:keys [key] :as ast}]
  (if-let [nested-shape (-> env
                            (assoc
                              ::pco/op-name key
                              :edn-query-language.ast/node ast)
                            (dissoc ::p.attr/attribute)
                            (compute-dynamic-nested-requirements))]
    (assoc ast ::foreign-ast (pfsd/shape-descriptor->ast nested-shape))
    ast))

(defn plan-mutation [graph env {:keys [key] :as ast}]
  (if-let [mutation (pci/mutation env key)]
    (let [ast (cond->> ast
                (-> mutation pco/operation-config ::pco/dynamic-name)
                (plan-mutation-nested-query env))]
      (update graph ::mutations coll/vconj ast))
    (update graph ::mutations coll/vconj ast)))

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
                      ; failed, collect unreachable
                      [(merge-unreachable graph graph') node-ids]))))

              ; process mutation
              (refs/kw-identical? (:type ast) :call)
              [(plan-mutation graph env ast) node-ids]

              :else
              [graph node-ids]))
          [graph #{}]
          (:children (:edn-query-language.ast/node env)))]
    (if (seq node-ids)
      (create-root-and graph' env node-ids)
      graph')))

(def keep-required-transducer
  (comp
    (remove (comp ::pco/optional? :params))
    (remove (comp #{'...} :query))
    (remove (comp #{'*} :key))
    (remove (comp int? :query))))

(defn required-ast-from-index-ast
  [{::keys [index-ast]}]
  {:type     :root
   :children (->> index-ast
                  (vals)
                  (into [] keep-required-transducer))})

(defn required-ast-from-source-ast
  [{::keys [source-ast]}]
  (update source-ast :children
    #(into (with-meta [] (meta %)) keep-required-transducer %)))

(defn verify-plan!*
  [env
   {::keys [unreachable-paths]
    :as    graph}]
  (if (seq unreachable-paths)
    (let [user-required (pfsd/ast->shape-descriptor (required-ast-from-index-ast graph))
          missing       (pfsd/intersection unreachable-paths user-required)]
      (if (seq missing)
        (let [path (get env ::p.path/path)]
          (throw
            (ex-info
              (cond-> (str "Pathom can't find a path for the following elements in the query: " (pr-str (pfsd/shape-descriptor->query missing)))
                path
                (str " at path " (pr-str path)))
              {::graph                          graph
               ::unreachable-paths              missing
               ::p.path/path                    path
               :com.wsscode.pathom3.error/phase ::plan
               :com.wsscode.pathom3.error/cause :com.wsscode.pathom3.error/attribute-unreachable})))
        graph))
    graph))

(defn verify-plan!
  "This will cause an exception to throw in case the plan can't reach some required
  attribute"
  [{:com.wsscode.pathom3.error/keys [lenient-mode?]
    :as                             env} graph]
  (if lenient-mode?
    (try
      (verify-plan!* env graph)
      (catch #?(:clj Throwable :cljs :default) _
        (assoc graph ::verification-failed? true)))
    (verify-plan!* env graph)))

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
            ::optimize-graph?
            ::plan-cache*])
    => ::graph]
   (compute-run-graph {} env))

  ([graph {::keys [optimize-graph?]
           :or    {optimize-graph? true}
           :as    env}]
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
                             ::snapshot-message "=== Start query plan ==="})

   (verify-plan!
     env
     (p.cache/cached ::plan-cache* env [(hash (::pci/index-oir env))
                                        (::available-data env)
                                        (:edn-query-language.ast/node env)
                                        (boolean optimize-graph?)]
       #(let [env' (-> (merge (base-env) env)
                       (vary-meta assoc ::original-env env))]
          (cond->
            (compute-run-graph*
              (merge (base-graph)
                     graph
                     {::index-ast          (pf.eql/index-ast (:edn-query-language.ast/node env))
                      ::source-ast         (:edn-query-language.ast/node env)
                      ::available-data     (::available-data env)
                      ::user-request-shape (pfsd/ast->shape-descriptor (:edn-query-language.ast/node env))})
              env')

            optimize-graph?
            (optimize-graph env')))))))

; endregion

; region graph optimizations

(defn can-merge-sibling-resolver-nodes?
  [graph node-id1 node-id2]
  (let [n1 (get-node graph node-id1)
        n2 (get-node graph node-id2)]
    (and
      ; is a resolver
      (::pco/op-name n1)
      ; same resolver
      (= (::pco/op-name n1) (::pco/op-name n2)))))

(defn optimize-AND-resolver-siblings
  [graph env parent-id pivot other-nodes]
  (let [matching-nodes (into #{}
                             (filter #(can-merge-sibling-resolver-nodes? graph pivot %))
                             other-nodes)
        merge-nodes    (sort (conj matching-nodes pivot))
        graph'         (cond-> graph
                         (seq matching-nodes)
                         (merge-sibling-resolver-nodes env parent-id merge-nodes))]
    [; optimize the new merged node
     (optimize-node graph' env (first merge-nodes))
     (into #{} (remove matching-nodes) other-nodes)]))

(defn optimize-AND-resolvers-pass
  "This pass will collapse the same resolver node branches. This also do a local optimization
  on AND's and OR's sub-nodes. This is important to simplify the pass to merge OR nodes."
  [graph env parent-id]
  (let [{::keys [run-and]} (get-node graph parent-id)]
    (loop [graph graph
           [pivot & node-ids] run-and]
      (if pivot
        (cond
          (get-node graph pivot ::pco/op-name)
          (let [[graph' node-ids'] (optimize-AND-resolver-siblings graph env parent-id pivot node-ids)]
            (recur graph' node-ids'))

          :else
          (recur
            (optimize-node graph env pivot)
            node-ids))
        graph))))

(defn optimize-branch-items [graph env branch-node-id]
  (let [branches (node-branches (get-node graph branch-node-id))]
    (reduce
      (fn [graph node-id]
        (optimize-node graph env node-id))
      graph
      branches)))

(defn optimize-AND-nested
  "This pass will look for branches of an AND node that are also AND nodes. In case
  further AND doesn't contain a run-next, it can be safely merged with the parent AND.

    AND           >        AND
    -> A          >        -> A
    -> AND        >        -> B
       -> B       >        -> C
       -> C       >
  "
  [graph env node-id]
  (let [{::keys [run-and]} (get-node graph node-id)]
    (loop [graph graph
           [pivot & node-ids] run-and]
      (if pivot
        (let [pivot-node (some->> pivot (get-node graph))]
          (if (and pivot-node
                   (::run-and pivot-node)
                   (nil? (::run-next pivot-node)))
            (let [_      (add-snapshot! graph env
                                        {::snapshot-event   ::snapshot-merge-nested-ands
                                         ::snapshot-message "Merge nested AND with parent AND"
                                         ::highlight-nodes  (into #{node-id pivot} (::run-and pivot-node))
                                         ::highlight-styles {node-id 1
                                                             pivot   1}})
                  graph' (-> (reduce
                               ; move each nested-and-child-node-id to parent AND
                               (fn [g nested-and-child-node-id]
                                 (-> g
                                     (remove-from-parent-branches {::node-id      nested-and-child-node-id
                                                                   ::node-parents #{pivot}})
                                     (add-branch-to-node node-id ::run-and nested-and-child-node-id)))
                               graph
                               (::run-and pivot-node))
                             (remove-node pivot))
                  _      (add-snapshot! graph' env
                                        {::snapshot-event   ::snapshot-merge-nested-ands-done
                                         ::snapshot-message "Merged nested AND with parent AND"
                                         ::highlight-nodes  (into #{node-id} (::run-and pivot-node))
                                         ::highlight-styles {node-id 1}})]
              (recur graph' node-ids))
            (recur graph node-ids)))
        graph))))

(defn compare-AND-children-denorm [node]
  (select-keys node [::pco/op-name ::run-and ::run-or ::run-next]))

(defn merge-sibling-equal-branches
  [graph env [target-node-id & mergeable-siblings-ids]]
  (add-snapshot! graph env
                 {::snapshot-message "Merge siblings of same type with same branches"
                  ::highlight-nodes  (into #{target-node-id} mergeable-siblings-ids)
                  ::highlight-styles {target-node-id 1}})
  (as-> graph <>
    (reduce
      (fn [graph node-id]
        (let [node         (get-node graph node-id)
              branch-items (node-branches node)]
          (reduce
            #(move-branch-item-node % target-node-id %2)
            graph
            branch-items)))
      <>
      mergeable-siblings-ids)
    (combine-run-next <> env (conj mergeable-siblings-ids target-node-id) target-node-id)
    (reduce (fn [graph node-id]
              (-> graph
                  (update-node target-node-id nil combine-expects (get-node graph node-id))
                  (remove-node node-id))) <> mergeable-siblings-ids)
    (add-snapshot! <> env {::snapshot-message "Merge done"
                           ::highlight-nodes  #{target-node-id}})))

(defn optimize-siblings-with-same-braches
  [graph env node-id]
  (let [branches           (-> (get-node graph node-id) node-branches)
        node-ids           (->> branches
                                (keep (fn [nid]
                                        (let [node (get-node graph nid)]
                                          (if (node-branch-type node) nid)))))
        denorm-index       (as-> graph <>
                             (assoc <> ::denorm-update-node compare-AND-children-denorm)
                             (reduce #(-> (update-in % [::nodes %2] dissoc ::run-next)
                                          (denormalize-node %2)) <> node-ids))
        same-branch-groups (->> node-ids
                                (group-by #(get-denormalized-node denorm-index %))
                                (coll/filter-vals #(> (count %) 1)))
        mergeable-groups   (vals same-branch-groups)]
    (when (= 15 node-id)
      (tap> ["NIDS" node-ids]))
    (if (seq mergeable-groups) (tap> ["G" mergeable-groups]))
    (if (seq mergeable-groups)
      (-> (reduce #(merge-sibling-equal-branches % env %2) graph mergeable-groups)
          (optimize-branch-items env node-id))
      graph)))

(defn optimize-AND-branches
  [graph env node-id]
  (-> graph
      (optimize-branch-items env node-id)
      (optimize-AND-nested env node-id)
      (optimize-AND-resolvers-pass env node-id)
      (optimize-siblings-with-same-braches env node-id)
      (simplify-branch-node env node-id)))

(defn sub-sequence? [seq-a seq-b]
  (->> (map = seq-a seq-b)
       (every? true?)))

(defn matching-chains? [chain-a chain-b]
  (sub-sequence?
    (map ::pco/op-name chain-a)
    (map ::pco/op-name chain-b)))

(defn merge-sibling-or-sub-chains [graph env [chain & other-chains]]
  (reduce
    (fn [graph chain']
      (let [{last-target-node-id ::node-id} (get chain (dec (count chain')))]
        (-> (reduce
              (fn [graph [{target-node-id ::node-id}
                          {source-node-id ::node-id}]]
                (-> graph
                    (add-snapshot! env {::snapshot-message "Merge sibling resolvers from same OR sub-path"
                                        ::highlight-nodes  #{target-node-id source-node-id}})
                    (merge-sibling-resolver-node target-node-id source-node-id)
                    (add-snapshot! env {::snapshot-message "Merged"
                                        ::highlight-nodes  #{target-node-id}})))
              graph
              (mapv vector chain chain'))
            (assoc-node last-target-node-id ::node-resolution-checkpoint? true))))
    graph
    other-chains))

(defn optimize-OR-sub-paths
  "This function looks to match branches of the OR node that are
  sub-paths of each other (eg: A, A -> B, merge to just A -> B). In this case we can
  merge those chains and return the number of branches in the OR node.

  At this moment this fn only deals with paths that have only resolvers,
  it may look for paths with sub-branches in the future."
  [graph env node-id]
  (let [{::keys [run-or]} (get-node graph node-id)
        resolver-chains
        (into #{}
              (keep
                (fn [node-id]
                  (let [chain (find-run-next-descendants graph
                                                         {::node-id node-id})]
                    (if (every? ::pco/op-name chain)
                      chain))))
              run-or)]
    (loop [graph graph
           [pivot & chains] resolver-chains]
      (if pivot
        (let [[graph' chains']
              (let [matching-chains (into #{}
                                          (filter #(matching-chains? % pivot))
                                          chains)
                    merge-chains    (->> (conj matching-chains pivot)
                                         (sort-by count #(compare %2 %)))
                    graph'          (cond-> graph
                                      (seq matching-chains)
                                      (merge-sibling-or-sub-chains env merge-chains))]
                [graph' (into #{} (remove matching-chains) chains)])]
          (recur graph' chains'))
        graph))))

(defn optimize-nested-OR [graph env node-id]
  (let [{::keys [run-or]} (get-node graph node-id)

        nested-candidates
        (into #{}
              (keep
                (fn [node-id]
                  (let [node (get-node graph node-id)]
                    (and (::run-or node)
                         (not (::run-next node))
                         node))))
              run-or)]
    (reduce
      (fn [graph {::keys   [run-or]
                  node-id' ::node-id}]
        (-> (add-snapshot! graph env {::snapshot-message "Pulling nested OR"
                                      ::highlight-nodes  (conj run-or node-id node-id')
                                      ::highlight-styles {node-id' 1}})
            (remove-node node-id')
            (add-node-branches node-id ::run-or run-or)
            (add-snapshot! env {::snapshot-message "Pulled nodes"
                                ::highlight-nodes  (conj run-or node-id)
                                ::highlight-styles {node-id 1}})))
      graph
      nested-candidates)))

(defn optimize-OR-branches [graph env node-id]
  (-> graph
      (optimize-branch-items env node-id)
      (optimize-OR-sub-paths env node-id)
      (optimize-nested-OR env node-id)
      (optimize-siblings-with-same-braches env node-id)
      (simplify-branch-node env node-id)))

(defn optimize-resolver-chain?
  [graph {::pco/keys [op-name] ::keys [run-next]}]
  (= op-name
     (get-node graph run-next ::pco/op-name)))

(defn optimize-resolver-chain
  "Merge node and its run-next, when they are the same dynamic resolver."
  [graph env node-id]
  (let [{::keys [run-next] :as node} (get-node graph node-id)
        next (get-node graph run-next)]
    (if (optimize-resolver-chain? graph node)
      (recur
        (-> graph
            (add-snapshot! env {::snapshot-message "Merge chained same dynamic resolvers."
                                ::highlight-nodes  #{node-id run-next}})
            (set-node-run-next node-id (::run-next next))
            (set-node-expects node-id (::expects next))
            (assoc-node node-id ::foreign-ast (::foreign-ast next))
            (update-node node-id nil dissoc ::source-op-name)
            (remove-node run-next))
        env
        node-id)
      graph)))

(defn optimize-resolver-node [graph env node-id]
  (let [{::keys [invalid-node?]} (get-node graph node-id)]
    (if invalid-node?
      (do
        (add-snapshot! graph env {::snapshot-message (str "Removing node " node-id " because it doesn't fulfill the sub-query.")
                                  ::highlight-nodes  #{node-id}})
        (remove-root-node-cluster graph [node-id]))
      (optimize-resolver-chain graph env node-id))))

(defn optimize-node
  [graph env node-id]
  (if-let [node (get-node graph node-id)]
    (do
      (add-snapshot! graph env {::snapshot-message (str "Visit node " node-id)
                                ::highlight-nodes  #{node-id}})
      (case (node-kind node)
        ::node-resolver
        (let [graph' (optimize-resolver-node graph env node-id)]
          (recur graph'
            env
            (get-node graph' node-id ::run-next)))

        ::node-and
        (recur
          (optimize-AND-branches graph env node-id)
          env
          (::run-next node))

        ::node-or
        (recur
          (optimize-OR-branches graph env node-id)
          env
          (::run-next node))))
    graph))

(defn optimize-graph
  [graph env]
  (-> graph
      (add-snapshot! env {::snapshot-message "=== Optimize ==="})
      (optimize-node env (::root graph))))

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
    (conj @snapshots* (assoc graph ::snapshot-message "Complete graph."))))
