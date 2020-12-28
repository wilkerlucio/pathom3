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

(defn map-select-entry
  [env source {:keys [key] :as ast}]
  (if-let [x (find source key)]
    (let [val (val x)
          ast (update ast :children #(or % [{:key          '*
                                             :dispatch-key '*}]))]
      (coll/make-map-entry
        key
        (cond
          (map? val)
          (map-select-ast env val ast)

          (or (sequential? val)
              (set? val))
          (into (empty val) (map #(map-select-ast env % ast)) val)

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

(>defn map-select-ast
  "Same as map-select, but using AST as source."
  [env source ast]
  [map? any? (s/keys :opt-un [:edn-query-language.ast/children])
   => any?]
  (if (map? source)
    (let [start (with-meta {} (meta source))]
      (into start (keep #(p.plugin/run-with-plugins env ::wrap-map-select-entry
                           map-select-entry env source %))
            (-> ast
                (pick-union-entry source)
                :children
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
