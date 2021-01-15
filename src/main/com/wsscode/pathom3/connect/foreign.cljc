(ns com.wsscode.pathom3.connect.foreign
  (:require
    [clojure.string :as str]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.error :as p.err]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [com.wsscode.pathom3.path :as p.path]
    [com.wsscode.pathom3.placeholder :as p.ph]
    [edn-query-language.core :as eql]))

(def index-query
  [{::pci/indexes
    [::pci/index-attributes
     ::pci/index-oir
     ::pci/index-io
     ::pci/idents
     ::pci/autocomplete-ignore
     ::pci/index-resolvers
     ::pci/index-mutations]}])

(defn remove-internal-keys [m]
  (into {} (remove (fn [[k _]] (str/starts-with? (or (namespace k) "") "com.wsscode.pathom"))) m))

(defn compute-foreign-input [{::pcp/keys [node] :as env}]
  (let [input  (::pcp/input node)
        entity (p.ent/entity env)]
    (select-keys entity (keys input))))

(defn compute-foreign-query
  [{::pcp/keys [node] :as env}]
  (let [inputs     (compute-foreign-input env)
        base-query (eql/ast->query (::pcp/foreign-ast node))]
    (if (seq inputs)
      (let [ident-join-key (if (= 1 (count inputs))
                             (first (keys inputs))
                             (-> (p.ph/find-closest-non-placeholder-parent-join-key env)
                                 pf.eql/ident-key))
            join-node      (if (contains? inputs ident-join-key)
                             [ident-join-key (get inputs ident-join-key)]
                             [::foreign-call nil])]
        {::base-query base-query
         ::query      [{(list join-node {:pathom/context (dissoc inputs ident-join-key)}) base-query}]
         ::join-node  join-node})
      {::base-query base-query
       ::query      base-query})))

(defn internalize-foreign-errors
  [{::p.path/keys [path]
    ::keys        [join-node]} errors]
  (coll/map-keys #(into (pop path) (cond-> % join-node next)) errors))

(defn call-foreign-parser [env parser]
  (let [{::keys [query join-node] :as foreign-call} (compute-foreign-query env)
        response (parser {} query)]
    (if-let [errors (::p.err/errors response)]
      (->> (internalize-foreign-errors (merge env foreign-call) errors)
           (swap! (::p.err/errors* env) merge)))
    (cond-> response join-node (get join-node))))

(defn internalize-parser-index*
  ([indexes] (internalize-parser-index* indexes nil))
  ([{::pci/keys [index-source-id] :as indexes} parser]
   (let [index-source-id (or index-source-id (gensym "dynamic-parser-"))]
     (-> indexes
         (update ::pci/index-resolvers
           (fn [resolvers]
             (into {}
                   (map (fn [[r v]] [r (assoc v ::pco/dynamic-name index-source-id)]))
                   resolvers)))
         (assoc-in [::pci/index-resolvers index-source-id]
           (pco/resolver index-source-id
             {::pco/cache?            false
              ::pco/dynamic-resolver? true}
             (fn [env _] (call-foreign-parser env parser))))
         (dissoc ::pci/index-source-id)))))

