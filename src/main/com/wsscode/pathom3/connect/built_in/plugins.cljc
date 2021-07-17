(ns com.wsscode.pathom3.connect.built-in.plugins
  (:require
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.connect.runner.async :as pcra]
    [com.wsscode.pathom3.interface.async.eql :as p.a.eql]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [com.wsscode.pathom3.plugin :as p.plugin]
    [com.wsscode.promesa.macros :refer [clet]]))

(defn ^:deprecated attribute-errors-plugin
  "DEPRECATED: attribute errors are now built-in, you can just remove it
  from your setup.

  This plugin makes attributes errors visible in the data."
  []
  {::p.plugin/id
   `attribute-errors-plugin})

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
