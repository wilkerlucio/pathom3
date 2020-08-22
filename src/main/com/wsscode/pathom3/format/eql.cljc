(ns com.wsscode.pathom3.format.eql
  "Helpers to manipulate EQL."
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.core :as misc]
    [com.wsscode.pathom3.attribute :as p.attr]
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
  (= :union (some-> ast :children first :type)))

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
  (misc/index-by :key children))

(defn map-select-entry
  [source {:keys [key children] :as ast}]
  (if-let [x (find source key)]
    (let [val (val x)]
      (misc/make-map-entry
        key
        (if children
          (cond
            (map? val)
            (map-select-ast val ast)

            (or (sequential? val)
                (set? val))
            (into (empty val) (map #(map-select-ast % ast)) val)

            :else
            val)
          val)))))

(>defn map-select-ast
  "Same as map-select, but using AST as source."
  [source {:keys [children]}]
  [any? (s/keys :opt-un [:edn-query-language.ast/children])
   => any?]
  (if (map? source)
    (into {} (keep #(map-select-entry source %)) children)
    source))

(>defn map-select
  "Starting from a map, do a EQL selection on that map. Think of this function as
  a power up version of select-keys.

  Example:
  (p/map-select {:foo \"bar\" :deep {:a 1 :b 2}} [{:deep [:a]}])
  => {:deep {:a 1}}"
  [source tx]
  [any? ::eql/query => any?]
  (map-select-ast source (eql/query->ast tx)))

(>defn data->shape
  "Helper function to transform a data into an output shape."
  [data]
  [any? => (? map?)]
  (if (map? data)
    (->> (reduce-kv
           (fn [out k v]
             (conj out
               (cond
                 (map? v)
                 {k (data->shape v)}

                 (sequential? v)
                 (let [shape (reduce
                               (fn [q x]
                                 (eql/merge-queries q (data->shape x)))
                               []
                               v)]
                   (if (seq shape)
                     {k shape}
                     k))

                 :else
                 k)))
           []
           data)
         ;optimize-empty-joins
         (sort-by (comp pr-str #(if (map? %) (ffirst %) %)))
         vec)))
