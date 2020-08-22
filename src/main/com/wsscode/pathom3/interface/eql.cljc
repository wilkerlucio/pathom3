(ns com.wsscode.pathom3.interface.eql
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [edn-query-language.core :as eql]))

(declare process-ast)

(defn prepare-process-env [env ast]
  (let [env' (merge (p.ent/with-cache-tree {} {})
                    env)]
    (assoc env'
      ::pcp/available-data (pfsd/data->shape-descriptor (p.ent/cache-tree env'))
      :edn-query-language.ast/node ast)))

(>defn process-ast
  [env ast]
  [(s/keys) :edn-query-language.ast/node => map?]
  (let [env   (prepare-process-env env ast)
        graph (pcp/compute-run-graph env)]
    (pcr/run-graph! (assoc env ::pcp/graph graph))
    (pf.eql/map-select-ast (p.ent/cache-tree env) ast)))

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


  By default the process will start with a blank cache tree, you can override it by
  changing sending a cache tree in the environment:

      (p.eql/process (-> (pci/register some-resolvers)
                         (p.ent/with-cache-tree {:eql \"initial data\"}))
        [:eql :request])

  For more options around processing check the docs on the connect runner."
  [env tx]
  [(s/keys) ::eql/query => map?]
  (process-ast env (eql/query->ast tx)))
