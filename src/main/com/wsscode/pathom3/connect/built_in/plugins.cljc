(ns com.wsscode.pathom3.connect.built-in.plugins
  (:require
    [clojure.walk :as walk]
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

   ::pcr/wrap-root-run-graph!
   (fn attribute-errors-plugin-wrap-run-graph-external [run-graph!]
     (fn attribute-errors-plugin-wrap-run-graph-internal [env ast-or-graph entity-tree*]
       (clet [entity (run-graph! env ast-or-graph entity-tree*)]
         (walk/postwalk
           (fn [x]
             (if (map? x)
               (p.error/process-entity-errors x)
               x))
           entity))))})

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
