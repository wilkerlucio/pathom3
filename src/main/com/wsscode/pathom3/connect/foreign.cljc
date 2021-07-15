(ns com.wsscode.pathom3.connect.foreign
  (:require
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.path :as p.path]
    [com.wsscode.promesa.macros :refer [clet ctry]]))

(def index-query
  [::pci/indexes])

(defn compute-foreign-input [{::pcp/keys [node] :as env}]
  (let [input  (::pcp/input node)
        entity (p.ent/entity env)]
    (select-keys entity (keys input))))

(defn compute-foreign-request
  [{::pcp/keys [node] :as env}]
  {:pathom/ast    (::pcp/foreign-ast node)
   :pathom/entity (compute-foreign-input env)})

(defn internalize-foreign-errors
  [{::p.path/keys [path]
    ::keys        [join-node]} errors]
  (coll/map-keys #(into (pop path) (cond-> % join-node next)) errors))

(defn call-foreign [env foreign]
  (let [foreign-call (compute-foreign-request env)]
    (foreign foreign-call)))

(pco/defresolver foreign-indexes-resolver [env _]
  {::pci/indexes
   (select-keys env
                [::pci/index-attributes
                 ::pci/index-oir
                 ::pci/index-io
                 ::pci/index-resolvers
                 ::pci/index-mutations
                 ::pci/transient-attrs
                 ::pci/index-source-id])})

(defn remove-foreign-indexes [indexes]
  (-> indexes
      (update ::pci/index-resolvers dissoc `foreign-indexes-resolver)
      (update ::pci/index-attributes dissoc ::pci/indexes)
      (update ::pci/index-oir dissoc ::pci/indexes)
      (update-in [::pci/index-io #{}] dissoc ::pci/indexes)))

(defn internalize-foreign-indexes
  "Introduce a new dynamic resolver and make all the resolvers in the index point to
  it."
  [{::pci/keys [index-source-id] :as indexes} foreign]
  (let [index-source-id (or index-source-id (gensym "foreign-pathom-"))]
    (-> indexes
        (remove-foreign-indexes)
        (update ::pci/index-mutations
          (fn [mutations]
            (coll/map-vals
              #(pco/update-config % assoc ::pco/dynamic-name index-source-id)
              mutations)))
        (update ::pci/index-resolvers
          (fn [resolvers]
            (coll/map-vals
              #(pco/update-config % assoc ::pco/dynamic-name index-source-id)
              resolvers)))
        (assoc-in [::pci/index-resolvers index-source-id]
          (pco/resolver index-source-id
            {::pco/cache?            false
             ::pco/dynamic-resolver? true}
            (fn [env _]
              (ctry
                (call-foreign env foreign)
                (catch #?(:clj Throwable :cljs :default) ex
                  (throw (ex-info (str "Foreign " index-source-id " exception: " (ex-message ex))
                                  {::p.path/path (::p.path/path env)}
                                  ex)))))))
        (dissoc ::pci/index-source-id)
        (assoc-in [::foreign-indexes index-source-id] indexes))))

(defn foreign-register
  "Load foreign indexes and incorporate it as an external data source. This will make
  every resolver from the remote to point to a single one, enabling data delegation
  to the foreign node.

  The return of this function is the indexes, you can use pci/register to add them
  into your environment."
  [foreign]
  (clet [{::pci/keys [indexes]} (foreign index-query)]
    (internalize-foreign-indexes indexes foreign)))
