(ns com.wsscode.pathom3.error
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.plugin :as p.plugin]
    #?(:cljs [goog.object :as gobj]))
  #?(:clj
     (:import
       (java.io
         PrintWriter
         StringWriter))))

(>def ::phase keyword?)

(>def ::cause
  (s/and keyword?
    #{::attribute-unreachable
      ::attribute-not-requested
      ::attribute-missing

      ::ancestor-error

      ::node-errors
      ::node-exception

      ::plugin-missing-id

      ::missing-output}))

(>def ::lenient-mode? boolean?)

(defn- optional? [index-ast attribute]
  (get-in index-ast [attribute :params :com.wsscode.pathom3.connect.operation/optional?]))

(defn attribute-node-error
  [{:com.wsscode.pathom3.connect.runner/keys [node-run-stats]
    ::p.attr/keys                            [attribute]
    ::pcp/keys                               [index-ast]
    :as                                      graph} node-id]
  (let [{:com.wsscode.pathom3.connect.runner/keys [node-error node-run-finish-ms]} (get node-run-stats node-id)]
    (cond
      node-error
      (coll/make-map-entry
        node-id
        {::cause     ::node-exception
         ::exception node-error})

      node-run-finish-ms
      (if-not (optional? index-ast attribute)
        (coll/make-map-entry
          node-id
          {::cause ::attribute-missing}))

      :else
      (if-let [[node-id' error] (->> (pcp/node-ancestors graph node-id)
                                     (keep (fn [node-id]
                                             (if-let [error (get-in node-run-stats [node-id :com.wsscode.pathom3.connect.runner/node-error])]
                                               [node-id error])))
                                     first)]
        (coll/make-map-entry
          node-id
          {::cause             ::ancestor-error
           ::error-ancestor-id node-id'
           ::exception         error})))))

(defn attribute-error
  "Return the attribute error, in case it failed."
  [response attribute]
  (if (contains? response attribute)
    nil
    (let [{:com.wsscode.pathom3.connect.planner/keys [index-ast index-attrs] :as run-stats}
          (-> response meta :com.wsscode.pathom3.connect.runner/run-stats)]
      (if (contains? index-ast attribute)
        (if-let [nodes (get index-attrs attribute)]
          (let [run-stats (assoc run-stats ::p.attr/attribute attribute)
                errors    (into {} (keep #(attribute-node-error run-stats %)) nodes)]
            (if (seq errors)
              {::cause              ::node-errors
               ::node-error-details errors}))
          (if-not (optional? index-ast attribute)
            {::cause ::attribute-unreachable}))
        {::cause ::attribute-not-requested}))))

(defn scan-for-errors? [response]
  ; some node error?
  ; something unreachable?

  ; is there a way to know if there wasn't any error without checking each attribute?

  ; check if map has meta
  (some-> response meta (contains? :com.wsscode.pathom3.connect.runner/run-stats)))

(defn process-entity-errors [env entity]
  (if (scan-for-errors? entity)
    (let [ast    (-> entity meta
                     :com.wsscode.pathom3.connect.runner/run-stats
                     :com.wsscode.pathom3.connect.planner/index-ast)
          errors (into {}
                       (keep (fn [k]
                               (if-let [error (p.plugin/run-with-plugins env ::wrap-attribute-error
                                                attribute-error entity k)]
                                 (coll/make-map-entry k error))))
                       (keys ast))]
      (cond-> entity
        (seq errors)
        (assoc :com.wsscode.pathom3.connect.runner/attribute-errors errors)))
    entity))

#?(:clj
   (defn error-stack [^Throwable err]
     (let [sw (StringWriter.)
           pw (PrintWriter. sw)]
       (.printStackTrace err pw)
       (.toString sw)))

   :cljs
   (defn error-stack [err]
     (gobj/get err "stack")))

(defn datafy-processor-error* [env]
  (-> env
      (select-keys [::error-message
                    ::error-stack
                    ::pcp/graph
                    :com.wsscode.pathom3.connect.runner/processor-error?
                    ;:com.wsscode.pathom3.connect.runner/processor-error-parent-env
                    :com.wsscode.pathom3.entity-tree/entity-tree
                    :com.wsscode.pathom3.path/path])
      (coll/update-if
        :com.wsscode.pathom3.connect.runner/processor-error-parent-env
        datafy-processor-error*)))

(defn datafy-processor-error [^Throwable err]
  (let [env (ex-data err)]
    (if (some-> env :com.wsscode.pathom3.connect.runner/processor-error?)
      (datafy-processor-error* env)
      {::error-message (ex-message err)
       ::error-stack   (error-stack err)})))
