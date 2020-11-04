(ns com.wsscode.pathom3.interface.eql
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.connect.runner.stats :as pcrs]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [com.wsscode.pathom3.interface.smart-map :as psm]
    [edn-query-language.core :as eql]))

(>defn process-ast
  [env ast]
  [(s/keys) :edn-query-language.ast/node => map?]
  (let [ent-tree* (get env ::p.ent/entity-tree* (atom {}))
        run-stats (pcr/run-graph! env ast ent-tree*)]
    (-> @ent-tree*
        (assoc ::pcr/run-stats (psm/smart-map
                                 (pcrs/run-stats-env run-stats)
                                 (-> run-stats
                                     (dissoc ::pcr/node-run-stats))))
        (pf.eql/map-select-ast ast))))

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

  By default the process will start with a blank entity tree, you can override it by
  changing sending a entity tree in the environment:

      (p.eql/process (-> (pci/register some-resolvers)
                         (p.ent/with-cache-tree {:eql \"initial data\"}))
        [:eql :request])

  For more options around processing check the docs on the connect runner."
  ([env tx]
   [(s/keys) ::eql/query => map?]
   (process-ast env (eql/query->ast tx)))
  ([env entity tx]
   [(s/keys) map? ::eql/query => map?]
   (process-ast (p.ent/with-entity env entity) (eql/query->ast tx))))
