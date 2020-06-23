(ns com.wsscode.pathom3.connect.operation
  (:require [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
            [clojure.spec.alpha :as s]))

; region protocols

(defprotocol IOperation
  (-operation-config [this]))

(defprotocol IResolver
  (-resolve [this env input])
  (-merge-result [this entity result]))

(defprotocol IMutate
  (-mutate [this env params]))

(defrecord SinglePropResolver [config resolve output-attr]
  IOperation
  (-operation-config [_] config)

  IResolver
  (-resolve [this env input]
    (resolve env input))

  (-merge-result [_ entity result]
    (assoc entity output-attr result))

  clojure.lang.IFn
  (invoke [this env input] (resolve env input)))

(defrecord MultiPropResolver [config resolve]
  IOperation
  (-operation-config [_] config)

  IResolver
  (-resolve [this env input]
    (resolve env input))

  (-merge-result [_ entity result]
    (merge entity result))

  clojure.lang.IFn
  (invoke [this env input] (resolve env input)))

; endregion

; region specs

(>def ::name symbol?)
(>def ::input vector?)
(>def ::output vector?)
(>def ::output-attribute keyword?)
(>def ::resolve fn?)
(>def ::resolver #(and (satisfies? IOperation %)
                       (satisfies? IResolver %)))

; endregion

; region constructors

(>defn resolver
  "Helper to return a resolver map"
  [name {::keys [output-attribute] :as config} resolve]
  [::name (s/keys :req [::output]) ::resolve => ::resolver]
  (let [config' (assoc config ::name name)]
    (if output-attribute
      (->SinglePropResolver config' resolve output-attribute)
      (->MultiPropResolver config' resolve))))

; endregion

; region macros

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

(>def ::operation-argument
  (s/or :sym symbol?
        :map ::map-destructure))

(>def ::operation-args
  (s/coll-of ::operation-argument :kind vector? :min-count 0 :max-count 2))

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
      (assoc ::output [output-attr] ::output-attribute output-attr)

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

(>def ::defresolver-args
  (s/and
    (s/cat :name symbol?
           :docstring (s/? string?)
           :arglist ::operation-args
           :output-attr (s/? keyword?)
           :options (s/? map?)
           :body (s/+ any?))
    (fn must-have-output-prop-or-options [{:keys [output-attr options]}]
      (or output-attr options))))

(defn full-symbol [sym ns]
  (if (namespace sym)
    sym
    (symbol (name (ns-name ns)) (name sym))))

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

  Also can add environment argument:

      (p/defresolver tao [env {:keys [pi]}] :tau (* 2 pi))

  Note that the defined function will always have arity 2, the macro will fill the
  missing arguments when you don't send.

  To clarify this one, when you write:

      (p/defresolver pi [] :pi 3.14)

  The generated fn will be:

      (fn [_ _] 3.14)

  The extended version:

      (p/defresolver tao [env {:keys [pi]}]
        {::p/input            [:pi]
         ::p/output           [:tao]
         ::p/output-attribute :tao}

  "
  {:arglists '([name docstring? arglist output-prop? options? & body])}
  [& args]
  (let [{:keys [name arglist body] :as params}
        (-> (s/conform ::defresolver-args args)
            (update :arglist normalize-arglist))

        arglist' (s/unform ::operation-args arglist)
        fqsym    (full-symbol name *ns*)]
    `(def ~name
       (resolver ~fqsym ~(params->resolver-options params)
         (fn ~arglist' ~@body)))))

(s/fdef defresolver
  :args ::defresolver-args
  :ret any?)

; endregion
