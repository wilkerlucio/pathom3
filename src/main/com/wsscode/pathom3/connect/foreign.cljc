(ns com.wsscode.pathom3.connect.foreign
  (:require
    [clojure.string :as str]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.path :as p.path]
    [com.wsscode.promesa.macros :refer [clet]]))

(def index-query
  [::pci/indexes])

(defn remove-internal-keys [m]
  (into {} (remove (fn [[k _]] (str/starts-with? (or (namespace k) "") "com.wsscode.pathom"))) m))

(defn compute-foreign-input [{::pcp/keys [node] :as env}]
  (let [input  (::pcp/input node)
        entity (p.ent/entity env)]
    (select-keys entity (keys input))))

(defn compute-foreign-query
  [{::pcp/keys [node] :as env}]
  {:pathom/ast    (::pcp/foreign-ast node)
   :pathom/entity (compute-foreign-input env)})

(defn internalize-foreign-errors
  [{::p.path/keys [path]
    ::keys        [join-node]} errors]
  (coll/map-keys #(into (pop path) (cond-> % join-node next)) errors))

(defn call-foreign [env parser]
  (let [foreign-call (compute-foreign-query env)
        response (parser foreign-call)]
    response))

(defn internalize-foreign-indexes
  ([{::pci/keys [index-source-id] :as indexes} foreign]
   (let [index-source-id (or index-source-id (gensym "dynamic-parser-"))]
     (-> indexes
         (update ::pci/index-resolvers
           (fn [resolvers]
             (coll/map-vals
               #(pco/update-config % assoc ::pco/dynamic-name index-source-id)
               resolvers)))
         (assoc-in [::pci/index-resolvers index-source-id]
           (pco/resolver index-source-id
             {::pco/cache?            false
              ::pco/dynamic-resolver? true}
             (fn [env _] (call-foreign env foreign))))
         (dissoc ::pci/index-source-id)))))

(defn foreign-register
  [foreign]
  (clet [{::pci/keys [indexes]} (foreign index-query)]
    (internalize-foreign-indexes indexes foreign)))
