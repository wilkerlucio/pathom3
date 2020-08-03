(ns com.wsscode.pathom3.format.eql
  "Helpers to manipulate EQL."
  (:require
    [edn-query-language.core :as eql]))

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
