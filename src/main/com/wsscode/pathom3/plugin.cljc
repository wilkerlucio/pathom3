(ns com.wsscode.pathom3.plugin
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.coll :as coll]))

(>def ::id "Plugin ID" symbol?)
(>def ::index-plugins (s/map-of ::id (s/keys :req [::id])))

(>def ::plugin-actions "Compiled list of actions for a given plugin type"
  (s/coll-of fn? :kind vector?))

(>def ::plugin-order (s/coll-of ::id :kind vector?))

(defn compile-extensions
  "Given a function and a list of extension wrappers, call then in order to create
  a composed functions of them."
  [f extension-wrappers]
  (reduce
    (fn [f wrapper]
      (wrapper f))
    f
    extension-wrappers))

(defn compile-env-extensions
  [env plugin-type f]
  (let [plugins (get-in env [::plugin-actions plugin-type])]
    (compile-extensions f plugins)))

(defn add-plugin
  "Add a new plugin to the end. This will create the appropriated structures to optimize
  the plugin call speed."
  ([plugin] (add-plugin {} plugin))
  ([env {::keys [id] :as plugin}]
   (let [env' (-> env
                  (assoc-in [::index-plugins id] plugin)
                  (update-in [::plugin-order] coll/vconj id))]
     (reduce-kv
       (fn [m k v] (update-in m [::plugin-actions k] coll/vconj v))
       env'
       (coll/filter-vals fn? plugin)))))

(defn run-with-plugins
  "Run some operation f wrapping it with the plugins of a given plugin-type installed
  in the environment."
  ([env plugin-type f]
   (let [augmented-v (compile-env-extensions env plugin-type f)]
     (augmented-v)))
  ([env plugin-type f a1]
   (let [augmented-v (compile-env-extensions env plugin-type f)]
     (augmented-v a1)))
  ([env plugin-type f a1 a2]
   (let [augmented-v (compile-env-extensions env plugin-type f)]
     (augmented-v a1 a2)))
  ([env plugin-type f a1 a2 a3]
   (let [augmented-v (compile-env-extensions env plugin-type f)]
     (augmented-v a1 a2 a3)))
  ([env plugin-type f a1 a2 a3 a4]
   (let [augmented-v (compile-env-extensions env plugin-type f)]
     (augmented-v a1 a2 a3 a4)))
  ([env plugin-type f a1 a2 a3 a4 a5]
   (let [augmented-v (compile-env-extensions env plugin-type f)]
     (augmented-v a1 a2 a3 a4 a5)))
  ([env plugin-type f a1 a2 a3 a4 a5 a6]
   (let [augmented-v (compile-env-extensions env plugin-type f)]
     (augmented-v a1 a2 a3 a4 a5 a6)))
  ([env plugin-type f a1 a2 a3 a4 a5 a6 a7]
   (let [augmented-v (compile-env-extensions env plugin-type f)]
     (augmented-v a1 a2 a3 a4 a5 a6 a7)))
  ([env plugin-type f a1 a2 a3 a4 a5 a6 a7 a8]
   (let [augmented-v (compile-env-extensions env plugin-type f)]
     (augmented-v a1 a2 a3 a4 a5 a6 a7 a8)))
  ([env plugin-type f a1 a2 a3 a4 a5 a6 a7 a8 & args]
   (apply (compile-env-extensions env plugin-type f) a1 a2 a3 a4 a5 a6 a7 a8 args)))
