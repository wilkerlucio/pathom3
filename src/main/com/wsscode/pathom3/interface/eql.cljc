(ns com.wsscode.pathom3.interface.eql
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.connect.foreign :as pcf]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.error :as p.error]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [com.wsscode.pathom3.plugin :as p.plugin]
    [edn-query-language.core :as eql]))

(>def :pathom/eql ::eql/query)
(>def :pathom/ast :edn-query-language.ast/node)
(>def :pathom/entity map?)
(>def :pathom/lenient-mode? ::p.error/lenient-mode?)

(defn select-ast-env [{::p.error/keys [lenient-mode?] :as env}]
  (cond-> env lenient-mode? (update ::pf.eql/map-select-include coll/sconj ::pcr/attribute-errors)))

(defn process-ast* [env ast]
  (let [ent-tree* (get env ::p.ent/entity-tree* (p.ent/create-entity {}))
        result    (pcr/run-graph! env ast ent-tree*)]
    (as-> result <>
      (pf.eql/map-select-ast (select-ast-env env) <> ast))))

(>defn process-ast
  [env ast]
  [(s/keys) :edn-query-language.ast/node => map?]
  (p.plugin/run-with-plugins env ::wrap-process-ast
    process-ast* env ast))

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

  By default, processing will start with a blank entity tree. You can override this by
  sending an entity tree as the second argument in the 3-arity version of this fn:

      (p.eql/process (pci/register some-resolvers)
        {:eql \"initial data\"}
        [:eql :request])

  For more options around processing check the docs on the connect runner."
  ([env tx]
   [(s/keys) ::eql/query => map?]
   (process-ast (assoc env ::pcr/root-query tx) (eql/query->ast tx)))
  ([env entity tx]
   [(s/keys) map? ::eql/query => map?]
   (assert (map? entity) "Entity data must be a map.")
   (process-ast (-> env
                    (assoc ::pcr/root-query tx)
                    (p.ent/with-entity entity))
                (eql/query->ast tx))))

(>defn process-one
  "Similar to process, but returns a single value instead of a map.

  This is a convenience method to read a single attribute.

  Simplest usage:
  ```clojure
  (p.eql/process-one env :foo)
  ```

  Same as process, you can send initial data:
  ```clojure
  (p.eql/process-one env {:data \"here\"} :foo)
  ```

  You can also use joins and param expressions:
  ```clojure
  (p.eql/process-one env {:join [:sub-query]})
  (p.eql/process-one env '(:param {:expr \"sion\"}))
  ```
  "
  ([env attr]
   [(s/keys)
    (s/or :prop ::eql/property
          :join ::eql/join
          :param ::eql/param-expr)
    => any?]
   (process-one env {} attr))
  ([env entity attr]
   [(s/keys)
    map?
    (s/or :prop ::eql/property
          :join ::eql/join
          :param ::eql/param-expr)
    => any?]
   (let [response (process env entity [attr])]
     (some-> response first val))))

(>defn satisfy
  "Works like process, but none of the original entity data is filtered out."
  [env entity tx]
  [(s/keys) map? ::eql/query => map?]
  (merge
    entity
    (process env entity tx)))

(>defn normalize-input
  "Normalize a remote interface input. In case of vector it makes a map. Otherwise
  returns as is.

  IMPORTANT: :pathom/tx is deprecated and its going to be dropped, if you are using it please
  replace it with :pathom/eql to avoid breakages in the future."
  [input]
  [(s/or :query ::eql/query
         :config (s/keys :req [(or :pathom/tx :pathom/eql :pathom/ast)] :opt [:pathom/entity]))
   => (s/keys :req [(or :pathom/tx :pathom/eql :pathom/ast)] :opt [:pathom/entity])]
  (if (vector? input)
    {:pathom/eql    input
     :pathom/entity {}}
    input))

(>defn extend-env
  [source-env env-extension]
  [map? (s/or :fn fn? :map map? :nil nil?) => map?]
  (if (fn? env-extension)
    (env-extension source-env)
    (merge source-env env-extension)))

(defn boundary-env [env request]
  (if-let [x (find request :pathom/lenient-mode?)]
    (assoc env ::p.error/lenient-mode? (val x))
    env))

(>defn boundary-interface
  "Returns a function that wraps the environment. When exposing Pathom to some external
  system, this is the recommended way to do it. The format here makes your API compatible
  with Pathom Foreign process, which allows the integration of distributed environments.

  When calling the remote interface the user can send a query or a map containing the
  query and the initial entity data. This map is open and you can use as a way to extend
  the API.

  Boundary interface:

  ([env-ext request])
  ([request])

  Request is one of:

  1. An EQL request
  2. A map, supported keys:
      :pathom/eql
      :pathom/ast
      :pathom/entity
      :pathom/lenient-mode?

  Env ext can be either a map to merge in the original env, or a function that transforms
  the env.
  "
  [env] [map? => fn?]
  (let [env' (pci/register env pcf/foreign-indexes-resolver)]
    (fn boundary-interface-internal
      ([env-extension input]
       (let [{:pathom/keys [eql entity ast] :as request} (normalize-input input)
             env'    (-> env'
                         (boundary-env input)
                         (extend-env env-extension)
                         (assoc ::source-request request))
             entity' (or entity {})]

         (if ast
           (process-ast (p.ent/with-entity env' entity') ast)
           (process env' entity' (or eql (:pathom/tx request))))))
      ([input]
       (boundary-interface-internal nil input)))))
