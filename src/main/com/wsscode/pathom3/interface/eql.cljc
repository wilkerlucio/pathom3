(ns com.wsscode.pathom3.interface.eql
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [edn-query-language.core :as eql]))

(defn prepare-process-env [{::keys [operations] :as env}]
  (let [defaults {::p.ent/cache-tree* (atom {})}]
    (-> (merge defaults env)
        (merge (pci/register {} operations)))))

(defn compute-plan [env tx]
  (pcp/compute-run-graph
    (assoc env
      ::pcp/available-data (pfsd/data->shape-descriptor (p.ent/entity env))
      :edn-query-language.ast/node (eql/query->ast tx))))

(>defn process [env tx]
  [(s/keys) ::eql/query
   => map?]
  (let [env' (prepare-process-env env)
        plan (compute-plan env' tx)]
    plan))
