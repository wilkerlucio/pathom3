(ns com.wsscode.pathom3.connect.indexes
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.core :as misc]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.format.eql :as pfse]))

(>def ::indexes map?)
(>def ::index-oir map?)
(>def ::index-resolvers map?)

(>def ::operations
  (s/or :single ::pco/operation
        :indexes ::indexes
        :many (s/coll-of ::operations)))

(defn merge-oir
  "Merge ::index-oir maps."
  [a b]
  (merge-with #(merge-with into % %2) a b))

(defmulti index-merger
  "This is an extensible gateway so you can define different strategies for merging different
  kinds of indexes."
  (fn [k _ _] k))

(defmethod index-merger ::index-oir [_ a b]
  (merge-oir a b))

(defmethod index-merger :default [_ a b]
  (misc/merge-grow a b))

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

(defn- register-resolver
  "Low level function to add resolvers to the index. This function adds the resolver
  configuration to the index set, adds the resolver to the ::pc/index-resolvers, add
  the output to input index in the ::pc/index-oir and the reverse index for auto-complete
  to the index ::pc/index-io."
  ([indexes resolver]
   (let [{::pco/keys [op-name input output]} (pco/operation-config resolver)
         input' (set input)]
     (merge-indexes indexes
                    {::index-resolvers {op-name resolver}
                     ::index-oir       (reduce (fn [indexes out-attr]
                                                 (cond-> indexes
                                                   (not= #{out-attr} input')
                                                   (update-in [out-attr input'] misc/sconj op-name)))
                                         {}
                                         (pfse/query-root-properties output))}))))

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

(>defn register
  "Add an operation to the indexes. The operation can be either a Resolver or a Mutation."
  ([operation-or-operations]
   [::operations => ::indexes]
   (register {} operation-or-operations))
  ([indexes operation-or-operations-or-indexes]
   [::indexes ::operations => ::indexes]
   (cond
     (sequential? operation-or-operations-or-indexes)
     (reduce register indexes operation-or-operations-or-indexes)

     (pco/operation? operation-or-operations-or-indexes)
     (case (pco/operation-type operation-or-operations-or-indexes)
       ::pco/operation-type-resolver
       (register-resolver indexes operation-or-operations-or-indexes))

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
