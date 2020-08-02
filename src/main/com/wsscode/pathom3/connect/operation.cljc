(ns com.wsscode.pathom3.connect.operation
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.pathom3.connect.operation.protocols :as pop]
    [com.wsscode.pathom3.entity :as pe]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]))

; region records

(defrecord SinglePropResolver [config resolve output-attr]
  pop/IOperation
  (-operation-config [_] config)
  (-operation-type [_] ::operation-type-resolver)

  pop/IResolver
  (-resolve [_ env input]
            (resolve env input))

  (-merge-result [_ entity result]
                 (assoc entity output-attr result))

  clojure.lang.IFn
  (invoke [_ env input] (resolve env input)))

(defrecord MultiPropResolver [config resolve]
  pop/IOperation
  (-operation-config [_] config)
  (-operation-type [_] ::operation-type-resolver)

  pop/IResolver
  (-resolve [_ env input]
            (resolve env input))

  (-merge-result [_ entity result]
                 (pe/merge-entity-data entity result))

  clojure.lang.IFn
  (invoke [this env input] (resolve env input)))

; endregion

; region specs

(>def ::name symbol?)
(>def ::input vector?)
(>def ::output vector?)
(>def ::output-attribute keyword?)
(>def ::resolve fn?)
(>def ::operation-type #{::operation-type-resolver})
(>def ::operation #(satisfies? pop/IOperation %))
(>def ::operation-config map?)
(>def ::resolver #(satisfies? pop/IResolver %))

; endregion

; region constructors and helpers

(>defn operation-config [operation]
  [::operation => ::operation-config]
  (pop/-operation-config operation))

(>defn operation-type [operation]
  [::operation => ::operation-type]
  (pop/-operation-type operation))

(>defn resolver
  "Helper to return a resolver map"
  [name {::keys [output output-attribute] :as config} resolve]
  [::name (s/keys :req [::output]) ::resolve => ::resolver]
  (let [config' (merge {::name     name
                        ::input    []
                        ::provides (pfsd/query->shape-descriptor output)}
                       config)]
    (if output-attribute
      (->SinglePropResolver config' resolve output-attribute)
      (->MultiPropResolver config' resolve))))

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
         (s/cat :name symbol?
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

(defn full-symbol [sym ns]
  (if (namespace sym)
    sym
    (symbol (name (ns-name ns)) (name sym))))

(defmacro defresolver
  "Defines a new Pathom resolver.

  Resolvers are the central abstraction around Pathom, a resolver is a function
  that contains some annotated information and follow a few rules:

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
