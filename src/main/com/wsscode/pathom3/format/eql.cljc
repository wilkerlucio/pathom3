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

(>def ::union-entry-key
  "When some data map contains this key, Pathom will use it to select which union path
  the processor will take."
  keyword?)

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

(defn prop [k] {:type :prop :dispatch-key k :key k})

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
  (-> ast (assoc :type :root) (dissoc :query)))

(defn union-key-on-data? [{:keys [union-key]} m]
  (contains? m union-key))

(defn pick-union-entry
  "Check if ast children is a union type. If so, makes a decision to choose a path and
  return that AST."
  [ast m]
  (if (union-children? ast)
    (let [meta-path (-> m meta ::union-entry-key)]
      (some (fn [{:keys [union-key] :as ast'}]
              (if (or (= union-key meta-path) (union-key-on-data? ast' m))
                (union->root ast')))
        (union-children ast)))
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
  (-> (into [] (remove #(-> % :type (= :call))) children)
      (->> (coll/index-by :key))
      (dissoc '*)))

(defn recursive-query? [query]
  (or (= '... query) (int? query)))

(defn map-select-entry
  [env source {:keys [key query type] :as ast}]
  (if-let [x (find source key)]
    (let [val (val x)
          ast (if (recursive-query? query) (:parent-ast ast) ast)
          ast (update ast :children #(or % [{:key          '*
                                             :dispatch-key '*}]))]
      (coll/make-map-entry
        key
        (cond
          (and (refs/kw-identical? type :call)
               (:com.wsscode.pathom3.connect.runner/mutation-error val))
          val

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

(defn stats-value?
  "Check if a value is a map with a run stats, or a sequence containing items
  with run stats"
  [x]
  (cond
    (map? x)
    (-> x meta (contains? :com.wsscode.pathom3.connect.runner/run-stats))

    (coll? x)
    (-> x first meta (contains? :com.wsscode.pathom3.connect.runner/run-stats))

    :else
    false))

(defn select-stats-data
  "Filter X to keep only values that are relevant for running stats."
  [x]
  (cond
    (map? x)
    (into (with-meta {} (meta x))
          (comp (filter
                  (fn [e]
                    (stats-value? (val e))))
                (map
                  (fn [e]
                    (coll/make-map-entry (key e) (select-stats-data (val e))))))
          x)

    (coll/collection? x)
    (cond-> (into (empty x)
                  (map select-stats-data)
                  x)
      (coll/coll-append-at-head? x)
      reverse)))

(>defn map-select-ast
  "Same as map-select, but using AST as source."
  [{::keys [map-select-include]
    :as    env} source ast]
  [map? any? (s/keys :opt-un [:edn-query-language.ast/children])
   => any?]
  (if (coll/native-map? source)
    (let [start    (with-meta {} (meta source))
          selected (into start
                         (keep #(p.plugin/run-with-plugins env ::wrap-map-select-entry
                                  map-select-entry env source (assoc % :parent-ast ast)))
                         (-> ast
                             (pick-union-entry source)
                             :children
                             (cond->
                               map-select-include
                               (include-extra-attrs map-select-include))
                             (cond->>
                               (ast-contains-wildcard? ast)
                               (extend-ast-with-wildcard source))))]
      (if (stats-value? source)
        (let [transient-attrs (into {}
                                    (comp (filter
                                            (fn [entry]
                                              (and (not (contains? selected (key entry)))
                                                   (stats-value? (val entry)))))
                                          (map
                                            (fn [entry]
                                              (coll/make-map-entry (key entry) (select-stats-data (val entry))))))
                                    source)]
          (vary-meta selected assoc-in
                     [:com.wsscode.pathom3.connect.runner/run-stats
                      :com.wsscode.pathom3.connect.runner/transient-stats]
                     transient-attrs))
        selected))
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
  (cond
    (map? data)
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

(defn seq-data->query [coll]
  (-> (data->query {::temp coll})
      ffirst val))

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
    (-> (or ast1 ast2)
        (cond->
          (seq idx')
          (assoc :children (map-children->children idx'))

          (and (seq idx') (not (contains? #{:join :root} (:type ast1))))
          (assoc :type :join))
        (dissoc :query))))

(defn merge-asts
  "Merges two ast's the difference between this and the original merge-ast is related to
  params, the original implementation would return nil when there is a mismatch in
  params between the entries being merged. This version will merge the params."
  ([] {:type :root
       :children []})
  ([q] q)
  ([qa qb]
   (reduce (fn [ast {:keys [key type params] :as item-b}]
             (if-let [[idx item] (->> ast :children
                                      (keep-indexed #(if (-> %2 :key (= key)) [%1 %2]))
                                      first)]
               (cond
                 (or (= :join (:type item) type)
                     (= :prop (:type item) type))
                 (cond-> (update-in ast [:children idx] merge-asts item-b)
                   (or params (:params item))
                   (update-in [:children idx :params] merge params))

                 (and (= :prop (:type item))
                      (= :join type))
                 (assoc-in ast [:children idx] item-b)

                 (= :call type)
                 (reduced nil)

                 :else ast)
               (update ast :children conj item-b)))
     qa
     (:children qb))))

(defn merge-queries
  "Merges two queries, the difference between this and the original merge is related to
  params, the original implementation would return nil when there is a mismatch in
  params between the entries being merged. This version will merge the params."
  [qa qb]
  (some-> (merge-asts (eql/query->ast qa) (eql/query->ast qb))
          (eql/ast->query)))
