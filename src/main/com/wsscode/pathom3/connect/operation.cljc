(ns com.wsscode.pathom3.connect.operation
  (:require
    [clojure.set :as set]
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef |]]
    [com.wsscode.misc.coll :as coll]
    #?(:clj [com.wsscode.misc.macros :as macros])
    [com.wsscode.misc.refs :as refs]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.operation.protocols :as pop]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    [edn-query-language.core :as eql])
  #?(:cljs
     (:require-macros
       [com.wsscode.pathom3.connect.operation])))

; region type predicates

(defn operation? [x] (satisfies? pop/IOperation x))
(defn resolver? [x] (satisfies? pop/IResolver x))
(defn mutation? [x] (satisfies? pop/IMutation x))

; endregion

; region specs

(>def ::op-name "Name of the operation" symbol?)
(>def ::input vector?)
(>def ::output vector?)
(>def ::params vector?)
(>def ::docstring string?)
(>def ::cache? boolean?)
(>def ::cache-store keyword?)
(>def ::cache-key fn?)
(>def ::batch? boolean?)
(>def ::priority int?)
(>def ::resolve fn?)
(>def ::mutate fn?)
(>def ::operation-type #{::operation-type-resolver ::operation-type-mutation})
(>def ::operation-config map?)
(>def ::operation operation?)
(>def ::resolver resolver?)
(>def ::mutation mutation?)
(>def ::provides ::pfsd/shape-descriptor)
(>def ::requires ::pfsd/shape-descriptor)
(>def ::optionals ::pfsd/shape-descriptor)
(>def ::dynamic-name ::op-name)
(>def ::dynamic-resolver? boolean?)
(>def ::transform fn?)

; endregion

; region records

(defrecord Resolver [config resolve]
  pop/IOperation
  (-operation-config [_] config)
  (-operation-type [_] ::operation-type-resolver)

  pop/IResolver
  (-resolve [_ env input] (resolve env input))

  #?@(:bb
      []

      :clj
      [clojure.lang.IFn
       (invoke [_this] (resolve {} {}))
       (invoke [_this input] (resolve {} input))
       (invoke [_this env input] (resolve env input))]

      :cljs
      [IFn
       (-invoke [_this] (resolve {} {}))
       (-invoke [_this input] (resolve {} input))
       (-invoke [_this env input] (resolve env input))]))

(defrecord Mutation [config mutate]
  pop/IOperation
  (-operation-config [_] config)
  (-operation-type [_] ::operation-type-mutation)

  pop/IMutation
  (-mutate [_ env input] (mutate env input))

  #?@(:bb
      []

      :clj
      [clojure.lang.IFn
       (invoke [_this] (mutate {} {}))
       (invoke [_this input] (mutate {} input))
       (invoke [_this env input] (mutate env input))]

      :cljs
      [IFn
       (-invoke [_this] (mutate {} {}))
       (-invoke [_this input] (mutate {} input))
       (-invoke [_this env input] (mutate env input))]))

; endregion

; region constructors and helpers

(>defn ?
  "Make an attribute optional"
  [attr]
  [::p.attr/attribute => any?]
  (eql/update-property-param attr assoc ::optional? true))

(>defn operation-config [operation]
  [::operation => ::operation-config]
  (pop/-operation-config operation))

(>defn operation-type [operation]
  [::operation => ::operation-type]
  (pop/-operation-type operation))

(defn describe-input*
  [ast path outs* opt-parent?]
  (doseq [{:keys [key params] :as node} (:children ast)]
    (let [opt? (or opt-parent? (::optional? params))]
      (if opt?
        (vswap! outs* assoc-in (concat [::optionals] path [key]) {})
        (vswap! outs* assoc-in (concat [::requires] path [key]) {}))
      (describe-input* node (conj path key) outs* opt?))))

(defn describe-input [input]
  (let [input-ast (eql/query->ast input)
        outs*     (volatile! {::requires {}})]
    (describe-input* input-ast [] outs* false)
    @outs*))

(defn- eql->root-attrs [eql]
  (->> eql eql/query->ast :children (into #{} (map :key))))

(defn input-destructure-missing [{::keys [input inferred-input]}]
  (if (and input
           inferred-input)
    (let [missing (set/difference
                    (eql->root-attrs inferred-input)
                    (eql->root-attrs input))]
      (if (seq missing) missing))))

(>defn resolver
  "Helper to create a resolver. A resolver have at least a name, the output definition
  and the resolve function.

  You can create a resolver using a map:

      (resolver
        {::op-name 'foo
         ::output  [:foo]
         ::resolve (fn [env input] ...)})

  Or with the helper syntax:

      (resolver 'foo {::output [:foo]} (fn [env input] ...))

  Returns an instance of the Resolver type.
  "
  ([op-name config]
   [::op-name (s/keys :opt [::input ::output ::params]) => ::resolver]
   (resolver (-> config
                 (coll/merge-defaults {::op-name op-name}))))
  ([op-name config resolve]
   [::op-name (s/keys :opt [::input ::output ::params]) ::resolve => ::resolver]
   (resolver (-> config
                 (coll/merge-defaults {::op-name op-name})
                 (assoc ::resolve resolve))))
  ([{::keys [transform op-name inferred-input input] :as config}]
   [(s/or :map (s/keys :req [::op-name] :opt [::input ::output ::resolve ::transform])
          :resolver ::resolver)
    => ::resolver]
   (let [config (if (resolver? config)
                  config
                  (cond-> config transform transform))]
     (when-not (s/valid? (s/keys) config)
       (s/explain (s/keys) config)
       (throw (ex-info (str "Invalid config on resolver " name)
                       {:explain-data (s/explain-data (s/keys) config)})))

     (if-not (::disable-validate-input-destructuring? config)
       (if-let [missing (input-destructure-missing config)]
         (throw (ex-info
                  (str "Input of resolver " op-name " destructuring requires attributes \"" (str/join "," missing) "\" that are not present at the input definition.")
                  {::input          input
                   ::inferred-input inferred-input}))))

     (if (resolver? config)
       config
       (let [{::keys [resolve output]} config
             defaults (if output
                        {::input    []
                         ::provides (pfsd/query->shape-descriptor output)}
                        {})

             {::keys [input] :as config'}
             (-> (merge defaults config)
                 (dissoc ::resolve ::transform))

             config'  (cond-> config'
                        input
                        (merge (describe-input input)))]
         (->Resolver config' (or resolve (fn [_ _]))))))))

(>defn mutation
  "Helper to create a mutation. A mutation must have a name and the mutate function.

  You can create a mutation using a map:

      (mutation
        {::op-name 'foo
         ::output  [:foo]
         ::mutate  (fn [env params] ...)})

  Or with the helper syntax:

      (mutation 'foo {} (fn [env params] ...))

  Returns an instance of the Mutation type.
  "
  ([op-name config]
   [::op-name (s/keys :opt [::output ::params]) => ::mutation]
   (mutation (-> config
                 (coll/merge-defaults {::op-name op-name}))))
  ([op-name config mutate]
   [::op-name (s/keys :opt [::output ::params]) ::mutate => ::mutation]
   (mutation (-> config
                 (coll/merge-defaults {::op-name op-name})
                 (assoc ::mutate mutate))))
  ([{::keys [transform] :as config}]
   [(s/or :map (s/keys :req [::op-name] :opt [::output ::mutate ::transform])
          :mutation ::mutation)
    => ::mutation]
   (when-not (s/valid? (s/keys) config)
     (s/explain (s/keys) config)
     (throw (ex-info (str "Invalid config on mutation " name)
                     {:explain-data (s/explain-data (s/keys) config)})))
   (if (mutation? config)
     config
     (let [{::keys [mutate output] :as config} (cond-> config transform transform)
           defaults (if output
                      {::provides (pfsd/query->shape-descriptor output)}
                      {})
           config'  (-> (merge defaults config)
                        (dissoc ::mutate ::transform))]
       (->Mutation config' (or mutate (fn [_ _])))))))

(>defn params
  "Pull parameters from environment. Always returns a map."
  [env]
  [map? => map?]
  (or (get-in env [:com.wsscode.pathom3.connect.planner/node
                   :com.wsscode.pathom3.connect.planner/params])
      {}))

(>defn with-node-params
  "Set current node params to params."
  ([params]
   [map? => map?]
   {:com.wsscode.pathom3.connect.planner/node
    {:com.wsscode.pathom3.connect.planner/params
     params}})

  ([env params]
   [map? map? => map?]
   (assoc-in env [:com.wsscode.pathom3.connect.planner/node
                  :com.wsscode.pathom3.connect.planner/params]
     params)))

; endregion

; region macros

#?(:clj
   (do
     (s/def ::simple-keys-binding
       (s/tuple #{:keys} (s/coll-of ident? :kind vector?)))

     (s/def ::qualified-keys-binding
       (s/tuple
         (s/and qualified-keyword? #(= (name %) "keys"))
         (s/coll-of simple-symbol? :kind vector?)))

     (s/def ::as-binding
       (s/tuple #{:as} simple-symbol?))

     (s/def ::map-destructure
       (s/every
         (s/or :simple-keys-binding ::simple-keys-binding
               :qualified-keys-bindings ::qualified-keys-binding
               :named-extract (s/tuple ::operation-argument keyword?)
               :as ::as-binding)
         :kind map?))

     (s/def ::operation-argument
       (s/or :sym symbol?
             :map ::map-destructure))

     (s/def ::operation-args
       (s/coll-of ::operation-argument :kind vector? :min-count 0 :max-count 2))

     (s/def ::defresolver-args
       (s/and
         (s/cat :name simple-symbol?
                :docstring (s/? string?)
                :arglist ::operation-args
                :options (s/? map?)
                :body (s/+ any?))
         (fn must-have-output-visible-map-or-options [{:keys [body options]}]
           (or (map? (last body)) options))))

     (s/def ::defmutation-args
       (s/and
         (s/cat :name simple-symbol?
                :docstring (s/? string?)
                :arglist ::operation-args
                :options (s/? map?)
                :body (s/+ any?)))))

   :cljs
   (s/def ::defresolver-args any?))

(defn as-entry? [x] (refs/kw-identical? :as (first x)))

(defn extract-destructure-map-keys-as-keywords [m]
  (into []
        (comp
          (remove as-entry?)
          (mapcat
            (fn [[k val]]
              (if (and (keyword? k)
                       (= "keys" (name k)))
                (map #(keyword (or (namespace %)
                                   (namespace k)) (name %)) val)
                [val]))))
        m))

(defn params->resolver-options [{:keys [arglist options body docstring]}]
  (let [[input-type input-arg] (last arglist)
        last-expr      (last body)
        inferred-input (if (refs/kw-identical? :map input-type)
                         (extract-destructure-map-keys-as-keywords input-arg))]
    (cond-> options
      (and (map? last-expr) (not (::output options)))
      (assoc ::output (pf.eql/data->query last-expr))

      inferred-input
      (assoc ::inferred-input inferred-input)

      (and inferred-input
           (not (::input options)))
      (assoc ::input inferred-input)

      docstring
      (assoc ::docstring docstring))))

(defn params->mutation-options [{:keys [arglist options body docstring]}]
  (let [[input-type params-arg] (last arglist)
        last-expr (last body)]
    (cond-> options
      (and (map? last-expr) (not (::output options)))
      (assoc ::output (pf.eql/data->query last-expr))

      (and (refs/kw-identical? :map input-type)
           (not (::params options)))
      (assoc ::params (extract-destructure-map-keys-as-keywords params-arg))

      docstring
      (assoc ::docstring docstring))))

(defn normalize-arglist
  "Ensures arglist contains two elements."
  [arglist]
  (loop [arglist arglist]
    (if (< (count arglist) 2)
      (recur (into '[[:sym _]] arglist))
      arglist)))

#?(:clj
   (defmacro defresolver
     "Defines a new Pathom resolver.

     Resolvers are the central abstraction around Pathom, a resolver is a function
     that contains some annotated information and follow a few rules:

     1. The resolver input must be a map, so the input information is labelled.
     2. A resolver must return a map, so the output information is labelled.
     3. A resolver also receives a separated map containing the environment information.

     Here are some examples of how you can use the defresolver syntax to define resolvers:

     The verbose example:

         (pco/defresolver song-by-id [env {:acme.song/keys [id]}]
           {::pco/input     [:acme.song/id]
            ::pco/output    [:acme.song/title :acme.song/duration :acme.song/tone]
            ::pco/params    []
            ::pco/transform identity}
           (fetch-song env id))

     The previous example demonstrates the usage of the most common options in defresolver.

     But we don't need to write all of that, for example, instead of manually saying
     the ::pco/input, we can let the defresolver infer it from the param destructuring, so
     the following code works the same (::pco/params and ::pco/transform also removed, since
     they were no-ops in this example):

         (pco/defresolver song-by-id [env {:acme.song/keys [id]}]
           {::pco/output [:acme.song/title :acme.song/duration :acme.song/tone]}
           (fetch-song env id))

     This makes for a cleaner write, now lets use this format and write a new example
     resolver:

         (pco/defresolver full-name [env {:acme.user/keys [first-name last-name]}]
           {::pco/output [:acme.user/full-name]}
           {:acme.user/full-name (str first-name \" \" last-name)})

     The first thing we see is that we don't use env, so we can omit it.

         (pco/defresolver full-name [{:acme.user/keys [first-name last-name]}]
           {::pco/output [:acme.user/full-name]}
           {:acme.user/full-name (str first-name \" \" last-name)})

     Also, when the last expression of the defresolver is a map, it will infer the output
     shape from it:

         (pco/defresolver full-name [{:acme.user/keys [first-name last-name]}]
           {:acme.user/full-name (str first-name \" \" last-name)})

     You can always override the implicit input and output by setting on the configuration
     map.

     Standard options:

       ::pco/output - description of resolver output, in EQL format
       ::pco/input - description of resolver input, in EQL format
       ::pco/params - description of resolver parameters, in EQL format
       ::pco/transform - a function to transform the resolver configuration before instantiating the resolver
       ::pcr/cache? - true by default, set to false to disable caching for the resolver

     Note that any other option that you send to the resolver config will be stored in the
     index and can be read from it at any time.

     The returned value is of the type Resolver, you can test your resolver by calling
     directly:

         (full-name {:acme.user/first-name \"Ada\"
                     :acme.user/last-name  \"Lovelace\"})
         => \"Ada Lovelace\"

     Note that similar to the way we define the resolver, we can also omit `env` (and even
     the input) when calling, the resolvers fns always support arity 0, 1 and 2.
     "
     {:arglists '([name docstring? arglist options? & body])}
     [& args]
     (let [{:keys [name docstring arglist body] :as params}
           (-> (s/conform ::defresolver-args args)
               (update :arglist normalize-arglist))

           arglist' (s/unform ::operation-args arglist)
           fqsym    (macros/full-symbol name (str *ns*))
           defdoc   (cond-> [] docstring (conj docstring))]
       `(def ~name
          ~@defdoc
          (resolver '~fqsym ~(params->resolver-options (assoc params ::op-name fqsym))
                    (fn ~name ~arglist'
                      ~@body))))))

#?(:clj
   (s/fdef defresolver
     :args ::defresolver-args
     :ret any?))

#?(:clj
   (defmacro defmutation
     "Defines a new Pathom mutation. The syntax of this macro is similar to defresolver,
     But where `defresolver` takes input, `defmutation` uses as ::params."
     {:arglists '([name docstring? arglist options? & body])}
     [& args]
     (let [{:keys [name docstring arglist body] :as params}
           (-> (s/conform ::defmutation-args args)
               (update :arglist normalize-arglist))

           arglist' (s/unform ::operation-args arglist)
           fqsym    (macros/full-symbol name (str *ns*))
           defdoc   (cond-> [] docstring (conj docstring))]
       `(def ~name
          ~@defdoc
          (mutation '~fqsym ~(params->mutation-options params)
                    (fn ~name ~arglist'
                      ~@body))))))

#?(:clj
   (s/fdef defmutation
     :args ::defmutation-args
     :ret any?))

(defn update-config
  "Returns a new resolver with the modified config. You can use this to change anything
  in the resolver configuration map. The only thing you can't change from here is the
  resolver or mutation functions. You can use the wrap-resolve and wrap-mutation
  helpers to do that."
  ([operation f]
   (update operation :config f))
  ([operation f a1]
   (update operation :config f a1))
  ([operation f a1 a2]
   (update operation :config f a1 a2))
  ([operation f a1 a2 a3]
   (update operation :config f a1 a2 a3))
  ([operation f a1 a2 a3 a4]
   (update operation :config f a1 a2 a3 a4))
  ([operation f a1 a2 a3 a4 a5]
   (update operation :config f a1 a2 a3 a4 a5))
  ([operation f a1 a2 a3 a4 a5 a6]
   (update operation :config f a1 a2 a3 a4 a5 a6))
  ([operation f a1 a2 a3 a4 a5 a6 a7]
   (update operation :config f a1 a2 a3 a4 a5 a6 a7))
  ([operation f a1 a2 a3 a4 a5 a6 a7 a8]
   (update operation :config f a1 a2 a3 a4 a5 a6 a7 a8))
  ([operation f a1 a2 a3 a4 a5 a6 a7 a8 a9]
   (update operation :config f a1 a2 a3 a4 a5 a6 a7 a8 a9))
  ([operation f a1 a2 a3 a4 a5 a6 a7 a8 a9 & args]
   (apply update operation :config f a1 a2 a3 a4 a5 a6 a7 a8 a9 args)))

(defn wrap-resolve
  "Return a new resolver with the resolve fn modified. You can use the previous fn or
  just replace. Here is a noop wrapper example:

  (wrap-resolve resolver
    (fn [resolve]
      (fn [env input]
        (resolve env input)))"
  [resolver f]
  (update resolver :resolve f))

(defn wrap-mutate
  "Return a new mutation with the resolve fn modified. You can use the previous fn or
  just replace. Here is a noop wrapper example:

  (wrap-mutate mutation
    (fn [mutate]
      (fn [env params]
        (mutate env params)))"
  [mutation f]
  (update mutation :mutate f))

(defn final-value? [x]
  (some-> x meta ::final true?))

; endregion
