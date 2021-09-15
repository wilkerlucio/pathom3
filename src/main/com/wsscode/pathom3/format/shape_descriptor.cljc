(ns com.wsscode.pathom3.format.shape-descriptor
  "Shape descriptor is a format to describe data. This format optimizes for fast detection
  of value present given a shape and a value path.

  This namespace contains functions to operate on maps in the shape descriptor format."
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.misc.refs :as refs]
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
     (with-meta (merge-with merge-shapes a b)
       (merge (meta a) (meta b)))

     (map? a) a
     (map? b) b

     :else b)))

(defn data->shape-descriptor
  "Helper function to transform a map into an shape descriptor.

  Edges of shape descriptor are always an empty map. If a value of the map is a sequence.
  This will combine the keys present in all items on the final shape description.

  WARN: this idea of merging is still under test, this may change in the future."
  [data]
  (if (map? data)
    (reduce-kv
      (fn [out k v]
        (assoc out
          k
          (cond
            (map? v)
            (data->shape-descriptor v)

            (sequential? v)
            (let [shape (reduce
                          (fn [q x]
                            (coll/merge-grow q (data->shape-descriptor x)))
                          {}
                          v)]
              (if (seq shape)
                shape
                {}))

            :else
            {})))
      {}
      data)))

(defn data->shape-descriptor-shallow
  "Like data->shape-descriptor, but only at the root keys of the data."
  [data]
  (zipmap (keys data) (repeat {})))

(>defn ast->shape-descriptor
  "Convert EQL AST to shape descriptor format."
  [ast]
  [:edn-query-language.ast/node => ::shape-descriptor]
  (reduce
    (fn [m {:keys [key type children params] :as node}]
      (if (refs/kw-identical? :union type)
        (let [unions (into [] (map ast->shape-descriptor) children)]
          (reduce merge-shapes m unions))
        (assoc m key (cond-> (ast->shape-descriptor node)
                       (seq params)
                       (vary-meta assoc ::params params)))))
    {}
    (:children ast)))

(>defn query->shape-descriptor
  "Convert pathom output format into shape descriptor format."
  [output]
  [:edn-query-language.core/query => ::shape-descriptor]
  (ast->shape-descriptor (eql/query->ast output)))

(>defn shape-descriptor->ast-children
  "Convert pathom output format into shape descriptor format."
  [shape]
  [::shape-descriptor => vector?]
  (let [union? (-> shape meta ::union?)]
    (if union?
      [{:type     :union
        :children (into []
                        (map (fn [[uk uv]]
                               {:type      :union-entry
                                :union-key uk
                                :children  (shape-descriptor->ast-children uv)}))
                        shape)}]

      (into []
            (map (fn [[k v]]
                   (let [params (-> v meta ::params)]
                     (cond-> {:type         :prop
                              :key          k
                              :dispatch-key k}
                       (seq v)
                       (assoc
                         :type :join
                         :children (shape-descriptor->ast-children v))

                       (seq params)
                       (assoc :params params)))))
            shape))))

(>defn shape-descriptor->ast
  "Convert pathom output format into shape descriptor format."
  [shape]
  [::shape-descriptor => map?]
  {:type     :root
   :children (shape-descriptor->ast-children shape)})

(>defn shape-descriptor->query
  "Convert pathom output format into shape descriptor format."
  [shape]
  [::shape-descriptor => (s/or :eql :edn-query-language.core/query
                               :union map?)]
  (let [union? (-> shape meta ::union?)]
    (into (if union?
            {}
            [])
          (map (fn [[k v]]
                 (let [params (-> v meta ::params)]
                   (cond-> (if (or (seq v) union?)
                             {k (shape-descriptor->query v)}
                             k)
                     (seq params)
                     (list params)))))
          shape)))

(defn relax-empty-collections
  "This helper will remove nested requirements when data is an empty collection. This
  allows for nested inputs with empty collections to still be valid in shape."
  [required data]
  (reduce
    (fn [r [k v]]
      (cond
        (and (contains? r k)
             (coll/collection? v)
             (empty? v))
        (assoc r k {})

        (and (contains? r k)
             (not= (get r k) {}))
        (update r k relax-empty-collections v)

        :else
        r))
    required
    (cond
      (map? data)
      data

      (coll/collection? data)
      (first data)

      :else
      nil)))

(>defn missing
  "Given some available and required shapes, returns which items are missing from available
  in the required. Returns nil when nothing is missing."
  ([available required]
   [::shape-descriptor ::shape-descriptor
    => (? ::shape-descriptor)]
   (let [res (into
               {}
               (keep (fn [el]
                       (let [attr      (key el)
                             sub-query (val el)]
                         (if (contains? available attr)
                           (if-let [sub-req (and (seq sub-query)
                                                 (missing (get available attr) sub-query))]
                             (coll/make-map-entry attr sub-req))
                           el))))
               required)]
     (if (seq res) res)))
  ([available required data]
   [::shape-descriptor ::shape-descriptor map? => (? ::shape-descriptor)]
   (missing available (relax-empty-collections required data))))

(>defn difference
  "Like set/difference, for shapes."
  [s1 s2]
  [(? ::shape-descriptor) (? ::shape-descriptor) => ::shape-descriptor]
  (reduce-kv
    (fn [out k sub]
      (if-let [x (find s2 k)]
        (let [v (val x)]
          (if (and (seq sub) (seq v))
            (let [sub-diff (difference sub v)]
              (if (seq sub-diff)
                (assoc out k sub-diff)
                out))
            out))
        (assoc out k sub)))
    (or (empty s1) {})
    s1))

(>defn intersection
  "Like set/intersection, for shapes."
  [s1 s2]
  [(? ::shape-descriptor) (? ::shape-descriptor) => ::shape-descriptor]
  (reduce-kv
    (fn [out k sub]
      (if-let [x (find s2 k)]
        (let [v    (val x)
              meta (merge (meta sub) (meta v))]
          (if (and (seq sub) (seq v))
            (let [sub-inter (intersection sub v)]
              (if (seq sub-inter)
                (assoc out k (with-meta sub-inter meta))
                (assoc out k (with-meta {} meta))))
            (assoc out k (with-meta {} meta))))
        out))
    (or (empty s1) {})
    s1))

(>defn select-shape
  "Select the parts of data covered by shape. This is similar to select-keys, but for
  nested shapes."
  [data shape]
  [map? ::shape-descriptor => map?]
  (reduce-kv
    (fn [out k sub]
      (if-let [x (find data k)]
        (let [v (val x)]
          (if (seq sub)
            (cond
              (map? v)
              (assoc out k (select-shape v sub))

              (coll/collection? v)
              (assoc out k (into (empty v) (map #(select-shape % sub)) v))

              :else
              (assoc out k v))
            (assoc out k v)))
        out))
    (empty data)
    shape))

(declare select-shape-filtering)

(defn- select-shape-filter-coll [out k v sub sub-req]
  (let [sub-keys (keys sub-req)]
    (assoc out k
      (into (empty v)
            (keep #(let [s' (select-shape-filtering % sub sub-req)]
                     (if (every? (fn [x] (contains? s' x)) sub-keys)
                       s'))) v))))

(>defn select-shape-filtering
  "Like select-shape, but in case of collections, if some item doesn't have all the
  required keys, its removed from the collection."
  ([data shape]
   [map? ::shape-descriptor => map?]
   (select-shape-filtering data shape shape))
  ([data shape required-shape]
   [map? ::shape-descriptor (? ::shape-descriptor) => map?]
   (reduce-kv
     (fn [out k sub]
       (if-let [x (find data k)]
         (let [v (val x)]
           (if (seq sub)
             (let [sub-req (get required-shape k)]
               (cond
                 (map? v)
                 (assoc out k (select-shape-filtering v sub sub-req))

                 (coll/collection? v)
                 (select-shape-filter-coll out k v sub sub-req)

                 :else
                 (assoc out k v)))
             (assoc out k v)))
         out))
     (empty data)
     shape)))
