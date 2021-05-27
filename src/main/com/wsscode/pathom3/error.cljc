(ns com.wsscode.pathom3.error
  (:require
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.connect.planner :as pcp]))

(>def ::error-type
  #{::attribute-unreachable
    ::attribute-not-requested
    ::node-errors
    ::node-exception
    ::attribute-missing
    ::ancestor-error})

(defn attribute-node-error
  [{:com.wsscode.pathom3.connect.runner/keys [node-run-stats] :as graph} node-id]
  (let [{:com.wsscode.pathom3.connect.runner/keys [node-error node-run-finish-ms]} (get node-run-stats node-id)]
    (coll/make-map-entry
      node-id
      (cond
        node-error
        {::error-type ::node-exception
         ::exception  node-error}

        node-run-finish-ms
        {::error-type ::attribute-missing}

        :else
        (let [[node-id error] (->> (pcp/node-ancestors graph node-id)
                                   (keep (fn [node-id]
                                           (if-let [error (get-in node-run-stats [node-id :com.wsscode.pathom3.connect.runner/node-error])]
                                             [node-id error])))
                                   first)]
          {::error-type        ::ancestor-error
           ::error-ancestor-id node-id
           ::exception         error})))))

(defn attribute-error
  "Return the attribute error, in case it failed."
  [response attribute]
  (let [{:com.wsscode.pathom3.connect.planner/keys [index-ast index-attrs] :as run-stats}
        (-> response meta :com.wsscode.pathom3.connect.runner/run-stats)]
    (if (contains? response attribute)
      nil
      (if (contains? index-ast attribute)
        (if-let [nodes (get index-attrs attribute)]
          {::error-type         ::node-errors
           ::node-error-details (into {} (map #(attribute-node-error run-stats %)) nodes)}
          {::error-type ::attribute-unreachable})
        {::error-type ::attribute-not-requested}))))
