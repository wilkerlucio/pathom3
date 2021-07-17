(ns com.wsscode.pathom3.connect.indexes
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.format.eql :as pfse]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]))

(>def ::indexes (s/keys))
(>def ::index-attributes map?)
(>def ::index-resolvers (s/map-of ::pco/op-name ::pco/resolver))
(>def ::index-mutations (s/map-of ::pco/op-name ::pco/mutation))
(>def ::index-source-id symbol?)

(>def ::index-oir
  "Index: Output -> Input -> Resolver"
  (s/map-of ::p.attr/attribute
            (s/map-of ::pfsd/shape-descriptor (s/coll-of ::pco/op-name :kind set?))))

(>def ::index-io "Index: Input -> Output"
  (s/map-of ::p.attr/attributes-set ::pfsd/shape-descriptor))

(>def ::operations
  (s/or :single ::pco/operation
        :indexes ::indexes
        :many (s/coll-of ::operations)))

(def op-set (s/coll-of ::pco/op-name :kind set?))

(>def ::attr-id ::p.attr/attribute)
(>def ::attr-combinations (s/coll-of ::pco/input :kind set?))
(>def ::attr-input-in op-set)
(>def ::attr-output-in op-set)
(>def ::attr-mutation-output-in op-set)
(>def ::attr-mutation-param-in op-set)

(>def ::transient-attrs
  "A set containing attributes that should be considered transient.

  A transit attribute should appear only in dynamic resolvers, their purpose is to work
  as linking points between attributes, but they are not valid attributes themselves.

  In other words, transient attributes reference other attributes, but they can't be
  queried."
  ::p.attr/attributes-set)

(declare resolver mutation)

(defn merge-oir
  "Merge ::index-oir maps."
  [a b]
  (merge-with #(merge-with into % %2) a b))

(defmulti index-merger
  "This is an extensible gateway so you can define different strategies for merging different
  kinds of indexes."
  (fn [k _ _] k))

(defmethod index-merger ::index-resolvers [_ a b]
  (reduce-kv
    (fn [m k v]
      (assert (not (contains? m k))
        (str "Tried to register duplicated resolver: " k))
      (assoc m k v))
    a
    b))

(defmethod index-merger ::index-mutations [_ a b]
  (reduce-kv
    (fn [m k v]
      (assert (not (contains? m k))
        (str "Tried to register duplicated mutation: " k))
      (assoc m k v))
    a
    b))

(defmethod index-merger ::index-oir [_ a b]
  (merge-oir a b))

(defmethod index-merger :default [_ a b]
  (coll/merge-grow a b))

(>defn merge-indexes
  "Merge index ib in index ia."
  [ia ib]
  [::indexes ::indexes => ::indexes]
  (reduce-kv
    (fn [idx k v]
      (if (contains? idx k)
        (update idx k #(index-merger k % v))
        (assoc idx k v)))
    ia ib))

(defn input-set [input]
  (into #{}
        (map (fn [attr]
               (if (map? attr)
                 (coll/map-vals input-set attr)
                 attr)))
        input))

(defn index-attributes [{::pco/keys [op-name requires provides]}]
  (let [input         (into #{} (keys requires))
        provides      (remove #(contains? input %) (keys provides))
        op-group      #{op-name}
        attr-provides (zipmap provides (repeat op-group))
        input-count   (count input)]
    (as-> ^:com.wsscode.pathom3.connect.runner/map-container? {} <>
      ; inputs
      (reduce
        (fn [idx in-attr]
          (update idx in-attr (partial merge-with coll/merge-grow)
            {::attr-id       in-attr
             ::attr-provides attr-provides
             ::attr-input-in op-group}))
        <>
        (case input-count
          0 [#{}]
          1 input
          [input]))

      ; combinations
      (if (> input-count 1)
        (reduce
          (fn [idx in-attr]
            (update idx in-attr (partial merge-with coll/merge-grow)
              {::attr-id           in-attr
               ::attr-combinations #{input}
               ::attr-input-in     op-group}))
          <>
          input)
        <>)

      ; provides
      (reduce
        (fn [idx out-attr]
          (if (vector? out-attr)
            (update idx (peek out-attr) (partial merge-with coll/merge-grow)
              {::attr-id        (peek out-attr)
               ::attr-reach-via {(into [input] (pop out-attr)) op-group}
               ::attr-output-in op-group})

            (update idx out-attr (partial merge-with coll/merge-grow)
              {::attr-id        out-attr
               ::attr-reach-via {input op-group}
               ::attr-output-in op-group})))
        <>
        provides)

      ; leaf / branches
      #_(reduce
          (fn [idx {:keys [key children]}]
            (cond-> idx
              key
              (update key (partial merge-with coll/merge-grow)
                {(if children ::attr-branch-in ::attr-leaf-in) op-group})))
          <>
          (if (map? output)
            (mapcat #(tree-seq :children normalized-children (eql/query->ast %)) (vals output))
            (tree-seq :children :children (eql/query->ast output)))))))

(defn- register-resolver
  "Low level function to add resolvers to the index. This function adds the resolver
  configuration to the index set, adds the resolver to the ::pc/index-resolvers, add
  the output to input index in the ::pc/index-oir and the reverse index for auto-complete
  to the index ::pc/index-io."
  ([indexes resolver]
   (let [{::pco/keys [op-name output requires dynamic-resolver?] :as op-config} (pco/operation-config resolver)
         input'     (into #{} (keys requires))
         root-props (pfse/query-root-properties output)]
     (assert (nil? (com.wsscode.pathom3.connect.indexes/resolver indexes op-name))
       (str "Tried to register duplicated resolver: " op-name))
     (merge-indexes indexes
       (cond->
         {::index-resolvers ^:com.wsscode.pathom3.connect.runner/map-container? {op-name resolver}}
         (not dynamic-resolver?)
         (assoc
           ::index-attributes (index-attributes op-config)
           ::index-io {input' (pfsd/query->shape-descriptor output)}
           ::index-oir (reduce (fn [indexes out-attr]
                                 (cond-> indexes
                                   (not (contains? requires out-attr))
                                   (update-in [out-attr requires] coll/sconj op-name)))
                         {}
                         root-props)))))))

(defn- register-mutation
  "Low level function to add a mutation to the index. For mutations, the index-mutations
  and the index-attributes are affected."
  ([indexes mutation]
   (let [{::pco/keys [op-name params output]} (pco/operation-config mutation)]
     (assert (nil? (com.wsscode.pathom3.connect.indexes/mutation indexes op-name))
       (str "Tried to register duplicated mutation: " op-name))
     (merge-indexes indexes
       {::index-mutations  ^:com.wsscode.pathom3.connect.runner/map-container? {op-name mutation}
        ::index-attributes (as-> {} <>
                             (reduce
                               (fn [idx attribute]
                                 (update idx attribute (partial merge-with coll/merge-grow)
                                   {::attr-id           attribute
                                    ::attr-mutation-param-in #{op-name}}))
                               <>
                               (some-> params pfse/query-root-properties))

                             (reduce
                               (fn [idx attribute]
                                 (update idx attribute (partial merge-with coll/merge-grow)
                                   {::attr-id            attribute
                                    ::attr-mutation-output-in #{op-name}}))
                               <>
                               (some-> output pfse/query-root-properties)))}))))

(>defn resolver
  [{::keys [index-resolvers]} resolver-name]
  [(s/keys :opt [::index-resolvers]) ::pco/op-name
   => (? ::pco/resolver)]
  (get index-resolvers resolver-name))

(>defn resolver-config
  "Given a indexes map and a resolver sym, returns the resolver configuration map."
  [{::keys [index-resolvers]} resolver-name]
  [(s/keys :opt [::index-resolvers]) ::pco/op-name
   => (? ::pco/operation-config)]
  (some-> (get index-resolvers resolver-name)
          (pco/operation-config)))

(>defn resolver-provides
  "Get the resolver provides from the resolver configuration map"
  [env resolver-sym]
  [(s/keys :opt [::index-resolvers]) ::pco/op-name
   => (? ::pco/provides)]
  (-> (resolver-config env resolver-sym)
      ::pco/provides))

(>defn resolver-optionals
  "Get the resolver provides from the resolver configuration map"
  [env resolver-sym]
  [(s/keys :opt [::index-resolvers]) ::pco/op-name
   => (? ::pco/optionals)]
  (-> (resolver-config env resolver-sym)
      ::pco/optionals))

(defn dynamic-resolver?
  [env resolver-name]
  (::pco/dynamic-resolver? (resolver-config env resolver-name)))

(>defn mutation
  [{::keys [index-mutations]} mutation-name]
  [(s/keys :opt [::index-mutations]) ::pco/op-name
   => (? ::pco/mutation)]
  (get index-mutations mutation-name))

(>defn mutation-config
  "Given a indexes map and a mutation sym, returns the mutation configuration map."
  [{::keys [index-mutations]} mutation-name]
  [(s/keys :opt [::index-mutations]) ::pco/op-name
   => (? ::pco/operation-config)]
  (some-> (get index-mutations mutation-name)
          (pco/operation-config)))

(>defn register
  "Add an operation to the indexes. The operation value supports some different types:

  Resolver: adds a single resolver
  Mutation: adds a single mutation
  Map: assumes its a map containing indexes, merges in using merge-indexes functionality
  Sequentials: a vector containing any of the operators (including other sequentials)"
  ([operation-or-operations]
   [::operations => ::indexes]
   (register {} operation-or-operations))
  ([indexes operation-or-operations-or-indexes]
   [::indexes ::operations => ::indexes]
   (cond
     (pco/operation? operation-or-operations-or-indexes)
     (case (pco/operation-type operation-or-operations-or-indexes)
       ::pco/operation-type-resolver
       (register-resolver indexes operation-or-operations-or-indexes)

       ::pco/operation-type-mutation
       (register-mutation indexes operation-or-operations-or-indexes))

     (sequential? operation-or-operations-or-indexes)
     (reduce register indexes operation-or-operations-or-indexes)

     (map? operation-or-operations-or-indexes)
     (merge-indexes indexes operation-or-operations-or-indexes)

     :else
     (throw (ex-info "Invalid type to register" {::operations operation-or-operations-or-indexes})))))

(defn operation-config [env op-name]
  (or (resolver-config env op-name)
      (mutation-config env op-name)))

(>defn attribute-available?
  "Check if some attribute is known in the index, this checks uses the index-oir."
  [{::keys [index-oir]} k]
  [(s/keys :req [::index-oir]) ::p.attr/attribute
   => boolean?]
  (contains? index-oir k))

(defn transient-attr? [{::keys [transient-attrs]} attr]
  (contains? transient-attrs attr))

(defn reachable-attributes*
  [{::keys [index-io] :as env} queue attributes]
  (if (seq queue)
    (let [[attr & rest] queue
          attrs (conj! attributes attr)]
      (recur env
        (into rest (remove #(contains? attrs %)) (-> index-io (get #{attr}) keys))
        attrs))
    attributes))

(defn reachable-attributes-for-groups*
  [{::keys [index-io]} groups attributes]
  (into #{}
        (comp (filter #(every? (fn [x] (contains? attributes x)) %))
              (mapcat #(-> index-io (get %) keys))
              (remove #(contains? attributes %)))
        groups))

(defn attrs-multi-deps
  [{::keys [index-attributes]} attrs]
  (into #{} (mapcat #(get-in index-attributes [% ::attr-combinations])) attrs))

(>defn reachable-attributes
  "Discover which attributes are available, given an index and a data context.

  Also includes the attributes from available-data."
  [{::keys [index-io] :as env} available-data]
  [(s/keys) map?
   => set?]
  (let [queue (-> #{}
                  (into (keys (get index-io #{})))
                  (into (keys available-data)))]
    (loop [attrs         (persistent! (reachable-attributes* env queue (transient #{})))
           group-reaches (reachable-attributes-for-groups* env (attrs-multi-deps env attrs) attrs)]
      (if (seq group-reaches)
        (let [new-attrs (persistent! (reachable-attributes* env group-reaches (transient attrs)))]
          (recur
            new-attrs
            (reachable-attributes-for-groups* env (attrs-multi-deps env new-attrs) new-attrs)))
        attrs))))

(defn reachable-paths*
  [{::keys [index-io] :as env} queue paths]
  (if (seq queue)
    (let [[[attr sub] & rest] queue
          attrs (update paths attr pfsd/merge-shapes sub)]
      (recur env
        (into rest
              (remove #(contains? attrs (key %)))
              (get index-io #{attr}))
        attrs))
    paths))

(defn reachable-paths-for-groups*
  [{::keys [index-io]} groups attributes]
  (->> (reduce
         (fn [group attr]
           (pfsd/merge-shapes group (get index-io attr)))
         {}
         (filterv #(every? (fn [x] (contains? attributes x)) %) groups))
       (coll/remove-keys #(contains? attributes %))))

(>defn reachable-paths
  "Discover which paths are available, given an index and a data context.

  Also includes the attributes from available-data."
  [{::keys [index-io] :as env} available-data]
  [(s/keys) ::pfsd/shape-descriptor
   => ::pfsd/shape-descriptor]
  (let [queue (pfsd/merge-shapes (get index-io #{}) available-data)]
    (loop [paths         (reachable-paths* env queue {})
           group-reaches (reachable-paths-for-groups* env (attrs-multi-deps env (keys paths)) paths)]
      (if (seq group-reaches)
        (let [new-attrs (reachable-paths* env group-reaches paths)]
          (recur
            new-attrs
            (reachable-paths-for-groups* env (attrs-multi-deps env (keys new-attrs)) new-attrs)))
        paths))))

(>defn attribute-reachable?
  "Discover which attributes are available, given an index and a data context."
  [env available-data attr]
  [(s/keys) map? ::p.attr/attribute
   => boolean?]
  (contains? (reachable-attributes env available-data) attr))
