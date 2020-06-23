(ns com.wsscode.pathom3.core
  (:require [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
            [clojure.spec.alpha :as s]))

(defprotocol IResolver
  (-resolve [this env input])
  (-merge-result [this entity result]))

(defrecord SinglePropResolver [config resolve output-prop]
  IResolver
  (-resolve [this env input]
    (resolve env input))

  (-merge-result [_ entity result]
    (assoc entity output-prop result))

  clojure.lang.IFn
  (invoke [this env input] (resolve env input)))

(defrecord MultiPropResolver [config resolve]
  IResolver
  (-resolve [this env input]
    (resolve env input))

  (-merge-result [_ entity result]
    (merge entity result))

  clojure.lang.IFn
  (invoke [this env input] (resolve env input)))

(>defn resolver
  "Helper to return a resolver map"
  [sym {::keys [transform] :as options} resolve]
  [symbol? map? fn? => map?]
  (cond-> (merge {::sym sym ::resolve resolve} options)
    transform transform))

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

(>def ::arg-destructure
  (s/or :sym symbol?
        :map ::map-destructure))

(>def ::operation-args
  (s/coll-of ::arg-destructure :kind vector? :min-count 0 :max-count 2))

(>def ::defresolver-args
  (s/and
    (s/cat :sym symbol?
           :docstring (s/? string?)
           :arglist ::operation-args
           :output-prop (s/? keyword?)
           :options (s/? map?)
           :body (s/+ any?))
    (fn must-have-output-prop-or-options [{:keys [output-prop options]}]
      (or output-prop options))))

(defmacro defresolver
  "Defines a new Pathom resolver.

  Resolvers are one of the central abstractions around Pathom, a resolver is a function
  that contains some annotated information, this fn also needs to follow a few rules:

  1. Every resolver takes two map arguments, the first being the env and the second the resolver input data.
  2. A resolver MUST return a map, so the output information is labelled.

  Here are some examples of how you can use the defresolver syntax to define resolvers:

  Defining a simple constant:

      (p/defresolver pi [] :pi 3.14)

  Define a dependent attribute:

      (p/defresolver tao [{:keys [pi]}] :tau (* 2 pi))

  Note that the required input was inferred from the param destructuring.

  The extended version:

      (p/defresolver tao [env {:keys [pi]}]
        {::p/input           [:pi]
         ::p/output          [:tao]
         ::p/output-property :tao}

  "
  {:arglists '([sym docstring? arglist output-prop? options? & body])}
  [sym arglist options & body]
  )

(defn init-env [env]
  env)
