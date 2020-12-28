(ns com.wsscode.pathom3.connect.indexes
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.format.eql :as pfse]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]))

(>def ::indexes map?)
(>def ::index-resolvers map?)
(>def ::index-oir map?)
(>def ::index-io map?)

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

(defn index-attributes [{::pco/keys [op-name input output]}]
  (let [input         (input-set input)
        provides      (remove #(contains? input %) (keys (pfsd/query->shape-descriptor output)))
        op-group      #{op-name}
        attr-provides (zipmap provides (repeat op-group))
        input-count   (count input)]
    (as-> {} <>
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
   (let [{::pco/keys [op-name input output] :as op-config} (pco/operation-config resolver)
         input' (input-set input)]
     (assert (nil? (com.wsscode.pathom3.connect.indexes/resolver indexes op-name))
       (str "Tried to register duplicated resolver: " op-name))
     (merge-indexes indexes
       {::index-resolvers  {op-name resolver}
        ::index-attributes (index-attributes op-config)
        ::index-io         {input' (pfsd/query->shape-descriptor output)}
        ::index-oir        (reduce (fn [indexes out-attr]
                                     (cond-> indexes
                                       (not= #{out-attr} input')
                                       (update-in [out-attr input'] coll/sconj op-name)))
                             {}
                             (pfse/query-root-properties output))}))))

(defn- register-mutation
  "Low level function to add a mutation to the index. For mutations, the index-mutations
  and the index-attributes are affected."
  ([indexes mutation]
   (let [{::pco/keys [op-name params output]} (pco/operation-config mutation)]
     (assert (nil? (com.wsscode.pathom3.connect.indexes/mutation indexes op-name))
       (str "Tried to register duplicated mutation: " op-name))
     (merge-indexes indexes
       {::index-mutations  {op-name mutation}
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
  "Add an operation to the indexes. The operation can be either a Resolver or a Mutation."
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

(>defn attribute-available?
  "Check if some attribute is known in the index, this checks uses the index-oir."
  [{::keys [index-oir]} k]
  [(s/keys :req [::index-oir]) ::p.attr/attribute
   => boolean?]
  (contains? index-oir k))

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

(>defn attribute-reachable?
  "Discover which attributes are available, given an index and a data context."
  [env available-data attr]
  [(s/keys) map? ::p.attr/attribute
   => boolean?]
  (contains? (reachable-attributes env available-data) attr))
