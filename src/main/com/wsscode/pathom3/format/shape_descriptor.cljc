(ns com.wsscode.pathom3.format.shape-descriptor
  "Shape descriptor is a format to describe data. This format optimizes for fast detection
  of value present given a shape and a value path.

  For example, "
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [edn-query-language.core :as eql]))

(>def ::shape-descriptor
  "Describes the shape of a nested map using maps, this is a way to efficiently check
  for the presence of a specific path on data."
  (s/map-of any? ::shape-descriptor))

(defn merge-shapes
  "Deep merge of shapes, it takes in account that values are always maps."
  ([a] a)
  ([a b]
   (cond
     (and (map? a) (map? b))
     (merge-with merge-shapes a b)

     (map? a) a
     (map? b) b

     :else b)))

(>defn ast->shape-descriptor
  "Convert EQL AST to shape descriptor format."
  [ast]
  [:edn-query-language.ast/node => ::shape-descriptor]
  (reduce
    (fn [m {:keys [key type children] :as node}]
      (if (= :union type)
        (let [unions (into [] (map ast->shape-descriptor) children)]
          (reduce merge-shapes m unions))
        (assoc m key (ast->shape-descriptor node))))
    {}
    (:children ast)))

(>defn query->shape-descriptor
  "Convert pathom output format into shape descriptor format."
  [output]
  [:edn-query-language.core/query => ::shape-descriptor]
  (ast->shape-descriptor (eql/query->ast output)))