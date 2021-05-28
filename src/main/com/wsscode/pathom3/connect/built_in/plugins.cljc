(ns com.wsscode.pathom3.connect.built-in.plugins
  (:require
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.connect.runner.async :as pcra]
    [com.wsscode.pathom3.error :as p.error]
    [com.wsscode.pathom3.interface.async.eql :as p.a.eql]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [com.wsscode.pathom3.plugin :as p.plugin]
    [com.wsscode.promesa.macros :refer [clet]]))

(defn attribute-errors-plugin
  "This plugin makes attributes errors visible in the data."
  []
  {::p.plugin/id
   `attribute-errors-plugin

   :com.wsscode.pathom3.interface.eql/wrap-process-ast
   (fn attribute-errors-plugin-wrap-process-ast-external [process]
     (fn attribute-errors-plugin-wrap-process-ast-internal [env ast]
       (process
         (update env :com.wsscode.pathom3.format.eql/map-select-include
           coll/sconj ::pcr/attribute-errors)
         ast)))

   ::pcr/wrap-run-graph!
   (fn attribute-errors-plugin-wrap-run-graph-external [run-graph!]
     (fn attribute-errors-plugin-wrap-run-graph-internal [env ast-or-graph entity-tree*]
       (clet [res (run-graph! env ast-or-graph entity-tree*)]
         (if (p.error/scan-for-errors? res)
           (let [ast    (-> res meta
                            :com.wsscode.pathom3.connect.runner/run-stats
                            :com.wsscode.pathom3.connect.planner/index-ast)
                 errors (into {}
                              (keep (fn [k]
                                      (if-let [error (p.error/attribute-error res k)]
                                        (coll/make-map-entry k error))))
                              (keys ast))]
             (cond-> res
               (seq errors)
               (assoc ::pcr/attribute-errors errors)))
           res))))})

(p.plugin/defplugin remove-stats-plugin
  "Remove the run stats from the result meta. Use this in production to avoid sending
  the stats. This is important for performance and security."
  {::pcr/wrap-run-graph!
   (fn remove-stats-plugin-wrap-run-graph-external [run-graph!]
     (fn remove-stats-plugin-wrap-run-graph-internal [env ast-or-graph entity-tree*]
       (clet [response (run-graph! env ast-or-graph entity-tree*)]
         (vary-meta response dissoc :com.wsscode.pathom3.connect.runner/run-stats))))})

(p.plugin/defplugin mutation-resolve-params
  "Remove the run stats from the result meta. Use this in production to avoid sending
  the stats. This is important for performance and security.

  TODO: error story is not complete, still up to decide what to do when params can't
  get fulfilled."
  {::pcr/wrap-mutate
   (fn mutation-resolve-params-external [mutate]
     (fn mutation-resolve-params-internal [env {:keys [key] :as ast}]
       (let [{::pco/keys [params]} (pci/mutation-config env key)]
         (clet [params' (if params
                          (if (::pcra/async-runner? env)
                            (p.a.eql/process env (:params ast) params)
                            (p.eql/process env (:params ast) params))
                          (:params ast))]
           (mutate env (assoc ast :params params'))))))})
