(ns com.wsscode.pathom3.error
  (:require
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.connect.planner :as pcp]))

(defn attribute-node-error
  [{:com.wsscode.pathom3.connect.runner/keys [node-run-stats] :as graph}
   attribute node-id]
  (let [{:com.wsscode.pathom3.connect.runner/keys [node-error node-run-finish-ms]} (get node-run-stats node-id)]
    (coll/make-map-entry node-id
                         (cond
                           node-error
                           {:type  :node-exception
                            :error node-error}

                           node-run-finish-ms
                           {:type      :attribute-missing
                            :attribute attribute}

                           :else
                           (let [[node-id error] (->> (pcp/node-ancestors graph node-id)
                                                      (keep (fn [node-id]
                                                              (if-let [error (get-in node-run-stats [node-id :com.wsscode.pathom3.connect.runner/node-error])]
                                                                [node-id error])))
                                                      first)]
                             {:type           :ancestor-error
                              :ancestor-id    node-id
                              :ancestor-error error})))))

(defn attribute-error
  "Return the attribute error, in case it failed."
  [response attribute]
  (let [{:com.wsscode.pathom3.connect.planner/keys [index-ast index-attrs] :as run-stats}
        (-> response meta :com.wsscode.pathom3.connect.runner/run-stats)]
    (if (contains? response attribute)
      nil
      (if (contains? index-ast attribute)
        (if-let [nodes (get index-attrs attribute)]
          {:error :node-errors
           :nodes (into {} (map #(attribute-node-error run-stats attribute %)) nodes)}
          {:error :attribute-unreachable})
        {:error :attribute-not-requested}))))
