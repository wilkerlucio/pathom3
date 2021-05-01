(ns com.wsscode.pathom3.interface.eql
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [com.wsscode.pathom3.plugin :as p.plugin]
    [edn-query-language.core :as eql]))

(defn process-ast* [env ast]
  (let [ent-tree* (get env ::p.ent/entity-tree* (p.ent/create-entity {}))
        result    (pcr/run-graph! env ast ent-tree*)]
    (as-> result <>
      (pf.eql/map-select-ast env <> ast))))

(>defn process-ast
  [env ast]
  [(s/keys) :edn-query-language.ast/node => map?]
  (p.plugin/run-with-plugins env ::wrap-process-ast
    process-ast* env ast))

(>defn process
  "Evaluate EQL expression.

  This interface allows you to request a specific data shape to Pathom and get
  the response as a map with all data combined.

  This is efficient for large queries, given Pathom can make a plan considering
  the whole request at once (different from Smart Map, which always plans for one
  attribute at a time).

  At minimum you need to build an index to use this.

      (p.eql/process (pci/register some-resolvers)
        [:eql :request])

  By default, processing will start with a blank entity tree. You can override this by
  sending an entity tree as the second argument in the 3-arity version of this fn:

      (p.eql/process (pci/register some-resolvers)
        {:eql \"initial data\"}
        [:eql :request])

  For more options around processing check the docs on the connect runner."
  ([env tx]
   [(s/keys) ::eql/query => map?]
   (process-ast (assoc env ::pcr/root-query tx) (eql/query->ast tx)))
  ([env entity tx]
   [(s/keys) map? ::eql/query => map?]
   (assert (map? entity) "Entity data must be a map.")
   (process-ast (-> env
                    (assoc ::pcr/root-query tx)
                    (p.ent/with-entity entity))
                (eql/query->ast tx))))

(>defn satisfy
  "Works like process, but none of the original entity data is filtered out."
  [env entity tx]
  [(s/keys) map? ::eql/query => map?]
  (merge
    entity
    (process env entity tx)))

(>defn normalize-input
  "Normalize a remote interface input. In case of vector it makes a map. Otherwise
  returns as is."
  [input]
  [(s/or :query ::eql/query
         :config (s/keys :req [:pathom/tx] :opt [:pathom/entity]))
   => (s/keys :req [:pathom/tx] :opt [:pathom/entity])]
  (if (vector? input)
    {:pathom/tx     input
     :pathom/entity {}}
    input))

(pco/defresolver foreign-indexes [env _]
  {::pco/output
   [{::pci/indexes
     [::pci/index-attributes
      ::pci/index-oir
      ::pci/index-io
      ::pci/autocomplete-ignore
      ::pci/index-resolvers
      ::pci/index-mutations]}]}
  {::pci/indexes
   (select-keys env
                [::pci/index-attributes
                 ::pci/index-oir
                 ::pci/index-io
                 ::pci/autocomplete-ignore
                 ::pci/index-resolvers
                 ::pci/index-mutations])})

(>defn foreign-interface
  "Returns a function that wraps the environment. When exposing Pathom to some external
  system, this is the recommended way to do it. The format here makes your API compatible
  with Pathom Foreign process, which allows the integration of distributed parsers.

  When calling the remote interface the user can send a query or a map containing the
  query and the initial entity data. This map is open and you can use as a way to extend
  the API."
  [env] [map? => fn?]
  (let [env' (pci/register env foreign-indexes)]
    (fn foreign-interface-internal [input]
      (let [{:pathom/keys [tx entity ast] :as request} (normalize-input input)
            env'    (assoc env' ::source-request request)
            entity' (or entity {})]

        (if ast
          (process-ast (p.ent/with-entity env' entity') ast)
          (process env' entity' tx))))))
