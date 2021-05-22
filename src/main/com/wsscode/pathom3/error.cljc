(ns main.com.wsscode.pathom3.error
  (:require
    [com.wsscode.pathom3.connect.runner :as pcr]))

(defn attribute-error
  "Return the attribute error, in case it failed."
  [response attribute]
  (let [_graph (-> response meta ::pcr/node-run-stats)]
    (if (contains? response attribute)
      nil)))
