(ns com.wsscode.pathom3.connect.foreign
  (:require
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.promesa.macros :refer [clet]]))

(def index-query
  [::pci/indexes])

(defn foreign-indexed-key [i]
  (keyword ">" (str "foreign-" i)))

(defn compute-foreign-request
  [inputs]
  (let [ph-requests (into []
                          (map-indexed
                            (fn [i {::pcp/keys [foreign-ast]
                                    ::pcr/keys [node-resolver-input]}]
                              (let [k (foreign-indexed-key i)]
                                (cond-> {:type         :join
                                         :key          k
                                         :dispatch-key k
                                         :children     (:children foreign-ast)}
                                  (seq node-resolver-input)
                                  (assoc :params node-resolver-input)))))
                          inputs)
        ast         {:type     :root
                     :children ph-requests}]
    {:pathom/ast ast}))

(defn compute-foreign-mutation
  [{::pcp/keys [node]}]
  {:pathom/ast (::pcp/foreign-ast node)})

(defn call-foreign-mutation [foreign {::pcp/keys [foreign-ast]}]
  (foreign {:pathom/ast foreign-ast}))

(defn call-foreign-query [foreign inputs]
  (clet [foreign-call (compute-foreign-request inputs)
         result       (foreign foreign-call)]
    (into
      []
      (map #(get result (foreign-indexed-key %)))
      (range (count inputs)))))

(defn call-foreign [foreign inputs]
  (if (-> inputs first ::pcp/foreign-ast :children first :type (= :call))
    (call-foreign-mutation foreign (first inputs))
    (call-foreign-query foreign inputs)))

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
      (update ::pci/index-mutations dissoc 'com.wsscode.pathom.viz.ws-connector.pathom3/request-snapshots)
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
             ::pco/batch?            true
             ::pco/dynamic-resolver? true}
            (fn [_env inputs]
              (call-foreign foreign inputs))))
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
