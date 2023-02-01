(ns com.wsscode.pathom3.connect.built-in.plugins
  (:require
    [com.fulcrologic.guardrails.core :refer [>def]]
    [com.wsscode.log :as l]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.misc.time :as time]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.connect.runner.async :as pcra]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [com.wsscode.pathom3.interface.async.eql :as p.a.eql]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [com.wsscode.pathom3.plugin :as p.plugin]
    [com.wsscode.promesa.macros :refer [clet ctry]]))

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

(>def ::apply-everywhere? boolean?)

(defn resolver-weight-tracker
  "Starts an atom to track the weight of a resolver. The weight is calculated by measuring
  the time a resolver takes to run. The time is add to last known time (or 1 in case of
  no previous data) and divided by two to get the new weight.

  You should use this plugin to enable weight sorting."
  []
  (let [weights* (atom {})]
    {::p.plugin/id
     `resolver-weight-tracker

     ::pcr/wrap-root-run-graph!
     (fn [process]
       (fn [env ast entity*]
         (process (coll/merge-defaults env {::pcr/resolver-weights* weights*}) ast entity*)))

     ::pcr/wrap-resolve
     (fn [resolve]
       (fn [{::pcr/keys [resolver-weights*]
             ::pcp/keys [node]
             :as        env} input]
         (let [{::pco/keys [op-name]} node]
           (ctry
             (clet [start   (time/now-ms)
                    result  (resolve env input)
                    elapsed (- (time/now-ms) start)]
               (swap! resolver-weights* update op-name #(/ (+ (or % 1) elapsed) 2))
               result)
             (catch #?(:clj Throwable :cljs :default) e
               (swap! resolver-weights* update op-name #(* (or % 10) 2))
               (throw e))))))}))

(defn filtered-sequence-items-plugin
  ([] (filtered-sequence-items-plugin {}))
  ([{::keys [apply-everywhere?]}]
   {::p.plugin/id
    `filtered-sequence-items-plugin

    ::pcr/wrap-process-sequence-item
    (fn [map-subquery]
      (fn [env ast m]
        (ctry
          (map-subquery env ast m)
          (catch #?(:clj Throwable :cljs :default) e
            (if (or apply-everywhere?
                    (-> ast :meta ::remove-error-items))
              nil
              (throw e))))))}))

(defn env-wrap-plugin
  "Plugin to help extend the environment with something dynamic. This will run once
  around the whole request.

      (p.plugin/register env (pbip/env-modify-plugin #(assoc % :data \"bar\")))"
  [env-modifier]
  {::p.plugin/id
   `env-wrap-plugin

   ::pcr/wrap-root-run-graph!
   (fn track-request-root-run-external [process]
     (fn track-request-root-run-internal [env ast entity*]
       (process
         (env-modifier env)
         ast
         entity*)))})

(defn dev-linter
  "This plugin adds linting features to help developers find sources of issues while
  Pathom runs its system.

  Checks done:

  - Verify if all the output that comes out of the resolver is declared in the resolver
    output. This means the user missed some attribute declaration in the resolver output
    and that may cause inconsistent behavior on planning/running."
  []
  {::p.plugin/id
   `dev-linter

   ::pcr/wrap-resolve
   (fn [resolve]
     (fn [env input]
       (clet [{::pco/keys [provides op-name]} (pci/resolver-config env (-> env ::pcp/node ::pco/op-name))
              res              (resolve env input)
              unexpected-shape (pfsd/difference (pfsd/data->shape-descriptor res) provides)]
         (if (seq unexpected-shape)
           (l/warn ::undeclared-output
                   {::pco/op-name      op-name
                    ::pco/provides     provides
                    ::unexpected-shape unexpected-shape}))
         res)))})

(defn placeholder-data-params
  "This plugin will make placeholder params change data from the entity they point to.
  This behavior used to happen by default in the past, but it's now provided in the form
  of this plugin.

      (p.plugin/register env (pbip/placeholder-data-params))

  Then you can do:

      (p.eql/process env [{'(:>/foo {:some-data \"value\"}) [:some-data]}]
      => {:some-data \"value\"}"
  []
  {::p.plugin/id
   `placeholder-data-params

   ::pcr/wrap-placeholder-merge-entity
   (fn placeholder-data-external [_]
     (fn placeholder-data-internal
       [{::pcp/keys [graph] ::pcr/keys [source-entity]}]
       (reduce
         (fn [out ph]
           (let [data (:params (pcp/entry-ast graph ph))]
             (assoc out ph (merge source-entity data))))
         {}
         (::pcp/placeholders graph))))})
