(ns com.wsscode.pathom3.plugin
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.coll :as coll]
    #?(:clj [com.wsscode.misc.macros :as macros]))
  #?(:cljs
     (:require-macros
       [com.wsscode.pathom3.plugin])))

(>def ::id "Plugin ID" symbol?)
(>def ::index-plugins (s/map-of ::id (s/keys :req [::id])))

(>def ::plugin (s/keys :req [::id]))
(>def ::plugins (s/coll-of ::plugin))

(>def ::plugin-or-plugins
  (s/or :one ::plugin :many
        (s/coll-of ::plugin-or-plugins)))

(>def ::plugin-actions "Compiled list of actions for a given plugin type"
  (s/map-of keyword? (s/coll-of fn? :kind vector?)))

(>def ::plugin-order (s/coll-of ::plugin :kind vector?))

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
  (if-let [plugins (get-in env [::plugin-actions plugin-type])]
    (compile-extensions f plugins)
    f))

(defn build-plugin-actions [{::keys [plugin-order index-plugins] :as env} k]
  (assoc-in env [::plugin-actions k]
    (into
      []
      (keep
        (fn [{::keys [id]}]
          (get-in index-plugins [id k])))
      (rseq plugin-order))))

(defn add-plugin-at-order
  [{::keys [plugin-order] :as env} {::keys [id add-before add-after]}]
  (assert (or (not (or add-before add-after))
              (not (and add-before add-after)))
    "You can provide add-before or add-after, but not both at the same time.")
  (let [ref-id       (or add-before add-after)
        ref-position (coll/index-of plugin-order {::id ref-id})]
    (cond-> env
      add-before
      (update-in [::plugin-order] coll/conj-at-index ref-position {::id id})

      add-after
      (update-in [::plugin-order] coll/conj-at-index (inc ref-position) {::id id})

      (not (or add-before add-after))
      (update-in [::plugin-order] coll/vconj {::id id}))))

(defn plugin-extensions [plugin]
  (keys (coll/filter-vals fn? plugin)))

(defn refresh-actions-from-plugin [env plugin]
  (reduce
    build-plugin-actions
    env
    (plugin-extensions plugin)))

(>defn register-plugin
  "Add a new plugin to the end. This will create the appropriated structures to optimize
  the plugin call speed."
  ([plugin]
   [::plugin => map?] (register-plugin {} plugin))
  ([env {::keys [id] :as plugin}]
   [map? ::plugin => map?]
   (assert (nil? (get-in env [::index-plugins id]))
     (str "Tried to add duplicated plugin: " id))
   (let [env' (-> env
                  (assoc-in [::index-plugins id] plugin)
                  (add-plugin-at-order plugin))]
     (refresh-actions-from-plugin env' plugin))))

(>defn register-before
  ([env ref-id plugin]
   [map? ::id ::plugin => map?]
   (register-plugin env (assoc plugin ::add-before ref-id))))

(>defn register-after
  ([env ref-id plugin]
   [map? ::id ::plugin => map?]
   (register-plugin env (assoc plugin ::add-after ref-id))))

(>defn register
  "Add one or many plugins."
  ([plugins] [::plugin-or-plugins => map?] (register {} plugins))
  ([env plugins]
   [map? ::plugin-or-plugins => map?]
   (cond
     (::id plugins)
     (register-plugin env plugins)

     (sequential? plugins)
     (reduce
       register
       env
       plugins)

     :else
     (throw
       (ex-info "Invalid plugin, make sure you set the ::p.plugin/id on it."
                {:plugin plugins})))))

(>defn remove-plugin
  "Remove a plugin."
  [env plugin-id]
  [map? ::id => map?]
  (if-let [{::keys [id] :as plugin} (get-in env [::index-plugins plugin-id])]
    (let [env' (-> env
                   (update ::index-plugins dissoc id)
                   (update ::plugin-order #(into [] (remove #{{::id id}}) %)))]
      (refresh-actions-from-plugin env' plugin))
    env))

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

#?(:clj
   (defmacro defplugin
     ([id doc options]
      (let [fqsym (macros/full-symbol id (str *ns*))]
        `(def ~id ~doc (merge {::id '~fqsym} ~options))))
     ([id options]
      (let [fqsym (macros/full-symbol id (str *ns*))]
        `(def ~id (merge {::id '~fqsym} ~options))))))
