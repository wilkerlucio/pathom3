(ns com.wsscode.pathom3.connect.operation
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.pathom3.connect.operation.protocols :as pop]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]))

; region specs

(>def ::op-name "Name of the operation" symbol?)
(>def ::input vector?)
(>def ::output vector?)
(>def ::resolve fn?)
(>def ::operation-type #{::operation-type-resolver})
(>def ::operation-config map?)
(>def ::operation #(satisfies? pop/IOperation %))
(>def ::resolver #(satisfies? pop/IResolver %))
(>def ::provides ::pfsd/shape-descriptor)
(>def ::dynamic-name ::op-name)
(>def ::dynamic-resolver? boolean?)

; endregion

; region records

(defrecord Resolver [config resolve]
  pop/IOperation
  (-operation-config [_] config)
  (-operation-type [_] ::operation-type-resolver)

  pop/IResolver
  (-resolve [_ env input] (resolve env input))

  clojure.lang.IFn
  (invoke [this env input] (resolve env input)))

; endregion

; region constructors and helpers

(>defn operation-config [operation]
  [::operation => ::operation-config]
  (pop/-operation-config operation))

(>defn operation-type [operation]
  [::operation => ::operation-type]
  (pop/-operation-type operation))

(>defn resolver
  "Helper to create a resolver. A resolver have at least a name, the output definition
  and the resolve function.

  You can create a resolver using a map:

  (resolver
    {::name    'foo
     ::output  [:foo]
     ::resolve (fn [env input] ...)})

  Or with the helper syntax:

  (resolver 'foo {::output [:foo]} (fn [env input] ...))

  Returns an instance of the Resolver type.
  "
  ([name config resolve]
   [::op-name (s/keys :opt [::output]) ::resolve => ::resolver]
   (resolver (assoc config ::op-name name ::resolve resolve)))
  ([{::keys [resolve output] :as config}]
   [(s/or :map (s/keys :req [::op-name] :opt [::output ::resolve])
          :resolver ::resolver) => ::resolver]
   (if (satisfies? pop/IResolver config)
     config
     (let [config' (->> (dissoc config ::resolve)
                        (merge (if output
                                 {::input    []
                                  ::provides (pfsd/query->shape-descriptor output)}
                                 {})))]
       (->Resolver config' (or resolve (fn [_ _])))))))

; endregion

; region macros

#?(:clj
   (do
     (s/def ::ns-keys-binding
       (s/tuple
         (s/and keyword? #(= (name %) "keys"))
         (s/coll-of simple-symbol? :kind vector?)))

     (s/def ::as-binding
       (s/tuple #{:as} simple-symbol?))

     (s/def ::map-destructure
       (s/every
         (s/or :keys-bindings ::ns-keys-binding
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
                :output-attr (s/? keyword?)
                :options (s/? map?)
                :body (s/+ any?))
         (fn must-have-output-prop-or-options [{:keys [output-attr options]}]
           (or output-attr options))))))

(defn as-entry? [x] (= :as (first x)))

(>defn extract-destructure-map-keys-as-keywords [m]
  [::map-destructure => (s/coll-of keyword? :kind vector?)]
  (into []
        (comp
          (remove as-entry?)
          (mapcat
            (fn [[k vals]]
              (map #(keyword (namespace k) (name %)) vals))))
        m))

(defn params->resolver-options [{:keys [arglist options output-attr]}]
  (let [[input-type input-arg] (last arglist)]
    (cond-> options
      output-attr
      (assoc ::output [output-attr])

      (and (= :map input-type)
           (not (::input options)))
      (assoc ::input (extract-destructure-map-keys-as-keywords input-arg)))))

(defn normalize-arglist
  "Ensures arglist contains two elements."
  [arglist]
  (loop [arglist arglist]
    (if (< (count arglist) 2)
      (recur (into '[[:sym _]] arglist))
      arglist)))

(defn full-symbol [sym ns]
  (if (namespace sym)
    sym
    (symbol ns (name sym))))

(defmacro defresolver
  "Defines a new Pathom resolver.

  Resolvers are the central abstraction around Pathom, a resolver is a function
  that contains some annotated information and follow a few rules:

  1. Every resolver takes two map arguments, the first being the env and the second the resolver input data.
  2. A resolver MUST return a map, so the output information is labelled.

  Here are some examples of how you can use the defresolver syntax to define resolvers:

  Defining a simple constant:

      (p/defresolver pi [] :pi 3.14)

  Define a resolver with dependent attribute:

      (p/defresolver tao [{:keys [pi]}] :tau (* 2 pi))

  Note that the required input was inferred from the param destructuring.

  To require multiple attributes:

      (p/defresolver user-by-id [env {:keys [user/id]}]
        {::p/output [:user/name :user/email]}
        (fetch-user-from-db env id))

  Note when we request 2 arguments in params, the first will be the environment.

  So far we seen examples using implicit input (inferred from input destructuring).

  If you want to control the input, you can add it to the resolver config:

      (p/defresolver user-by-id [env {:keys [user/id]}]
        {::p/input  [:user/id]
         ::p/output [:user/name :user/email]}
        (fetch-user-from-db env id))
  "
  {:arglists '([name docstring? arglist output-prop? options? & body])}
  [& args]
  (let [{:keys [name arglist body output-attr] :as params}
        (-> (s/conform ::defresolver-args args)
            (update :arglist normalize-arglist))

        arglist' (s/unform ::operation-args arglist)
        fqsym    (full-symbol name (str *ns*))]
    `(def ~name
       (resolver '~fqsym ~(params->resolver-options params)
                 (fn ~name ~arglist'
                   ~(if output-attr
                      `{~output-attr (do ~@body)}
                      `(do ~@body)))))))

(s/fdef defresolver
  :args ::defresolver-args
  :ret any?)

; endregion
