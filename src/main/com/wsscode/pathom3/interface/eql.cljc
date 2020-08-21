(ns com.wsscode.pathom3.interface.eql
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.core :as misc]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.specs :as p.spec]
    [edn-query-language.core :as eql]))

(declare process-ast)

(>def ::output-tree* ::p.ent/entity-tree*)

(defn process-entry [env {:keys [key children] :as ast}]
  (let [val (get (p.ent/entity env) key)]
    (misc/make-map-entry key
                         (if children
                           (cond
                             (map? val)
                             (process-ast (update env ::p.spec/path conj key) ast)

                             :else
                             val)
                           val))))

(defn prepare-process-env [env ast]
  (let [env' (merge (-> {::p.spec/path []}
                        (p.ent/with-cache-tree {}))
                    env)]
    (assoc env'
      ::pcp/available-data (pfsd/data->shape-descriptor (p.ent/entity env'))
      :edn-query-language.ast/node ast)))

(>defn process-ast
  [env ast]
  [(s/keys) :edn-query-language.ast/node => map?]
  (let [env   (prepare-process-env env ast)
        graph (pcp/compute-run-graph env)]
    (pcr/run-graph! (assoc env ::pcp/graph graph))
    (into {}
          (map #(process-entry env %))
          (:children ast))))

(>defn process
  [env tx]
  [(s/keys) ::eql/query => map?]
  (process-ast env (eql/query->ast tx)))
