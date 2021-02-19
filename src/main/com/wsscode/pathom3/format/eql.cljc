(ns com.wsscode.pathom3.format.eql
  "Helpers to manipulate EQL."
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.misc.refs :as refs]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.plugin :as p.plugin]
    [edn-query-language.core :as eql]))

(declare map-select-ast)

(>def ::prop->ast (s/map-of any? :edn-query-language.ast/node))
(>def ::map-select-include ::p.attr/attributes-set)

(defn query-root-properties
  "Returns a vector with the properties at the root of the query.

  For example:

    (query-root-properties [{:a [:b]} :c])
    => [:a :c]

  In case the query is a union query, it will merge the roots of then will merge:

    (query-root-properties {:foo [{:a [:b]} :c]
                            :bar [:a :d]})
    => [:a :c :d]"
  [query]
  (if (map? query)
    (into [] (distinct) (apply concat (map query-root-properties (vals query))))
    (->> query eql/query->ast :children (mapv :key))))

(defn union-children?
  "Given an AST point, check if the children is a union query type."
  [ast]
  (refs/kw-identical? :union (some-> ast :children first :type)))

(defn union-children
  "Get union children when its an union, otherwise return nil."
  [ast]
  (if (union-children? ast)
    (-> ast :children first :children)))

(defn union->root
  "Convert a union entry to a root."
  [ast]
  (-> ast (assoc :type :root) (dissoc :union-key :query)))

(defn union-key-on-data? [{:keys [union-key]} m]
  (contains? m union-key))

(defn pick-union-entry
  "Check if ast children is a union type. If so, makes a decision to choose a path and
  return that AST."
  [ast m]
  (if (union-children? ast)
    (some (fn [ast']
            (if (union-key-on-data? ast' m)
              (union->root ast')))
      (union-children ast))
    ast))

(defn maybe-merge-union-ast
  "Check if AST entry is a union, if so it computes a new AST entry by combining
  all union paths as a single entry."
  [ast]
  (if (union-children? ast)
    (let [merged-children (into [] (mapcat :children) (some-> ast :children first :children))]
      (assoc ast
        :children merged-children
        :query (eql/ast->query {:type :root :children merged-children})))
    ast))

(>defn ident-key
  "When key is an ident, return the first part of it. Otherwise returns nil."
  [key]
  [any? => (? ::p.attr/attribute)]
  (if (vector? key) (first key)))

(>defn index-ast [{:keys [children]}]
  [:edn-query-language.ast/node => ::prop->ast]
  ; TODO consider merging issues when key is repeated
  (coll/index-by :key children))

(defn recursive-query? [query]
  (or (= '... query) (int? query)))

(defn map-select-entry
  [env source {:keys [key query] :as ast}]
  (if-let [x (find source key)]
    (let [val (val x)
          ast (if (recursive-query? query) (:parent-ast ast) ast)
          ast (update ast :children #(or % [{:key          '*
                                             :dispatch-key '*}]))]
      (coll/make-map-entry
        key
        (cond
          (map? val)
          (map-select-ast env val ast)

          (coll/collection? val)
          (into (empty val) (map #(map-select-ast env % ast))
                (cond-> val
                  (coll/coll-append-at-head? val)
                  reverse))

          :else
          val)))))

(defn ast-contains-wildcard?
  "Check if some of the AST children is the wildcard value, which is *."
  [{:keys [children]}]
  (boolean (some #{'*} (map :key children))))

(defn extend-ast-with-wildcard [source children]
  (let [children-contains? (fn [k] (->> children
                                        (filter (comp #{k} :key))
                                        first
                                        boolean))]
    (reduce
      (fn [children k]
        (if (children-contains? k)
          children
          (conj children {:type         :prop
                          :key          k
                          :dispatch-key k})))
      children
      (keys source))))

(defn include-extra-attrs [children attrs]
  (into children (map (fn [k] {:type         :prop
                               :key          k
                               :dispatch-key k})) attrs))

(>defn map-select-ast
  "Same as map-select, but using AST as source."
  [{::keys [map-select-include] :as env} source ast]
  [map? any? (s/keys :opt-un [:edn-query-language.ast/children])
   => any?]
  (if (coll/native-map? source)
    (let [start (with-meta {} (meta source))]
      (into start (keep #(p.plugin/run-with-plugins env ::wrap-map-select-entry
                           map-select-entry env source (assoc % :parent-ast ast)))
            (-> ast
                (pick-union-entry source)
                :children
                (cond->
                  map-select-include
                  (include-extra-attrs map-select-include))
                (cond->>
                  (ast-contains-wildcard? ast)
                  (extend-ast-with-wildcard source)))))
    source))

(>defn map-select
  "Starting from a map, do a EQL selection on that map. Think of this function as
  a power up version of select-keys.

  Example:
  (p/map-select {:foo \"bar\" :deep {:a 1 :b 2}} [{:deep [:a]}])
  => {:deep {:a 1}}"
  [env source tx]
  [map? any? ::eql/query => any?]
  (map-select-ast env source (eql/query->ast tx)))

(>defn data->query
  "Helper function to transform a data into an output shape."
  [data]
  [any? => (? ::eql/query)]
  (if (map? data)
    (->> (reduce-kv
           (fn [out k v]
             (if (or (keyword? k)
                     (eql/ident? k))
               (conj out
                 (cond
                   (map? v)
                   (let [q (data->query v)]
                     (if (seq q)
                       {k q}
                       k))

                   (sequential? v)
                   (let [shape (reduce
                                 (fn [q x]
                                   (eql/merge-queries q (data->query x)))
                                 []
                                 v)]
                     (if (seq shape)
                       {k shape}
                       k))

                   :else
                   k))
               out))
           []
           data)
         (sort-by (comp pr-str #(if (map? %) (ffirst %) %))) ; sort results
         vec)))

(defn map-children->children [map-children]
  (reduce-kv
    (fn [children _ ast]
      (conj children (cond-> ast (:children ast) (update :children map-children->children))))
    []
    map-children))

(defn merge-ast-children [ast1 ast2]
  (let [idx  (coll/index-by :key (:children ast1))
        idx' (reduce
               (fn [idx {:keys [key] :as node}]
                 (let [node (dissoc node :query)]
                   (if (contains? idx key)
                     (update idx key merge-ast-children node)
                     (assoc idx key node))))
               idx
               (:children ast2))]
    (-> (merge ast2 ast1)
        (cond->
          (seq idx')
          (assoc :children (map-children->children idx'))

          (and (seq idx') (not (contains? #{:join :root} (:type ast1))))
          (assoc :type :join))
        (dissoc :query))))
