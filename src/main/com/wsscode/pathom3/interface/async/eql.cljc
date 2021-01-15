(ns com.wsscode.pathom3.interface.async.eql
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [#?(:clj  com.wsscode.async.async-clj
        :cljs com.wsscode.async.async-cljs)
     :as w.async
     :refer [go-promise go-catch <? <?maybe <!maybe]]
    [com.wsscode.pathom3.connect.runner.async :as pcra]
    [com.wsscode.pathom3.connect.runner.async-promesa :as pcrap]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [edn-query-language.core :as eql]
    [promesa.core :as p]))

(>defn process-ast
  [env ast]
  [(s/keys) :edn-query-language.ast/node => map?]
  (go-promise
    (let [ent-tree* (get env ::p.ent/entity-tree* (p.ent/create-entity {}))
          result    (<? (pcra/run-graph! env ast ent-tree*))]
      (as-> result <>
        (pf.eql/map-select-ast env <> ast)))))

(>defn process
  "Evaluate EQL expression using async runner.

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
   (process-ast env (eql/query->ast tx)))
  ([env entity tx]
   [(s/keys) map? ::eql/query => map?]
   (process-ast (p.ent/with-entity env entity) (eql/query->ast tx))))

(>defn process-ast-promesa
  [env ast]
  [(s/keys) :edn-query-language.ast/node => p/promise?]
  (p/let [ent-tree* (get env ::p.ent/entity-tree* (p.ent/create-entity {}))
          result    (pcrap/run-graph! env ast ent-tree*)]
    (as-> result <>
      (pf.eql/map-select-ast env <> ast))))

(>defn process-promesa
  "Evaluate EQL expression using async runner.

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
   [(s/keys) ::eql/query => p/promise?]
   (process-ast-promesa env (eql/query->ast tx)))
  ([env entity tx]
   [(s/keys) map? ::eql/query => p/promise?]
   (process-ast-promesa (p.ent/with-entity env entity) (eql/query->ast tx))))
