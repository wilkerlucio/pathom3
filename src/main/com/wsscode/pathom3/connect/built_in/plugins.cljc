(ns com.wsscode.pathom3.connect.built-in.plugins
  (:require
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.connect.runner.stats :as pcrs]
    [com.wsscode.pathom3.interface.smart-map :as psm]
    [com.wsscode.pathom3.plugin :as p.plugin]
    [com.wsscode.promesa.macros :refer [clet]]))

(defn attribute-errors-plugin
  "This plugin makes attributes errors visible in the data."
  []
  {::p.plugin/id
   `attribute-errors-plugin

   :com.wsscode.pathom3.interface.eql/wrap-process-ast
   (fn [process]
     (fn [env ast]
       (process
         (update env :com.wsscode.pathom3.format.eql/map-select-include
           coll/sconj ::pcr/attribute-errors)
         ast)))

   :com.wsscode.pathom3.connect.runner/wrap-run-graph!
   (fn [run-graph!]
     (fn [env ast-or-graph entity-tree*]
       (clet [res (run-graph! env ast-or-graph entity-tree*)]
             (let [stats       (-> res meta :com.wsscode.pathom3.connect.runner/run-stats)
                   smart-stats (psm/smart-run-stats stats)
                   ast         (-> stats :com.wsscode.pathom3.connect.planner/index-ast)
                   errors      (into {}
                                     (keep (fn [k]
                                             (if-let [error (pcrs/get-attribute-error smart-stats k)]
                                               (coll/make-map-entry k (::pcr/node-error error)))))
                                     (keys ast))]
               (cond-> res
                 (seq errors)
                 (assoc ::pcr/attribute-errors errors))))))})

(p.plugin/defplugin remove-stats-plugin
  "Remove the run stats from the result meta. Use this in production to avoid sending
  the stats. This is important for performance and security."
  {:com.wsscode.pathom3.connect.runner/wrap-run-graph!
   (fn [run-graph!]
     (fn [env ast-or-graph entity-tree*]
       (clet [response (run-graph! env ast-or-graph entity-tree*)]
             (vary-meta response dissoc :com.wsscode.pathom3.connect.runner/run-stats))))})
