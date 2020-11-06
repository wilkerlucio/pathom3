(ns com.wsscode.pathom3.connect.runner.stats
  (:require
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]))

; region performance

(pco/defresolver resolver-accumulated-duration
  [{::pcr/keys [node-run-stats]}]
  {::resolver-accumulated-duration-ms
   (transduce (map ::pcr/run-duration-ms) + 0 (vals node-run-stats))})

(pco/defresolver overhead-duration
  [{::pcr/keys [graph-process-duration-ms]
    ::keys     [resolver-accumulated-duration-ms]}]
  {::overhead-duration-ms
   (- graph-process-duration-ms resolver-accumulated-duration-ms)})

(pco/defresolver overhead-pct
  [{::pcr/keys [graph-process-duration-ms]
    ::keys     [overhead-duration-ms]}]
  {::overhead-duration-percentage
   (double (/ overhead-duration-ms graph-process-duration-ms))})

; endregion

; region errors

(pco/defresolver attribute-node
  [{::pcp/keys [index-attrs]}
   {::p.attr/keys [attribute]}]
  {::pcp/node-id (get index-attrs attribute)})

(pco/defresolver attribute-error
  "Find the error for a node, it first try to find the error in the node itself, but
  also walks up the graph to collect errors on previous nodes."
  [{::pcr/keys [node-run-stats] :as env}
   {::pcp/keys [node-id]}]
  {::pco/output [::attribute-error]}
  (let [error (get-in node-run-stats [node-id ::pcr/node-error])]
    (if error
      {::attribute-error
       {::node-error-type ::node-error-type-direct
        ::pcr/node-error  error}}

      (if-let [error (->> (pcp/node-ancestors env node-id)
                          (some #(get-in node-run-stats [% ::pcr/node-error])))]
        {::attribute-error
         {::node-error-type ::node-error-type-ancestor
          ::pcr/node-error  error}}))))

; endregion

(def stats-registry
  [resolver-accumulated-duration
   overhead-duration
   overhead-pct
   attribute-node
   attribute-error
   (pbir/env-table-resolver ::pcp/nodes ::pcp/node-id
     [::pco/op-name
      ::pcp/expects
      ::pcp/input
      ::pcp/run-and
      ::pcp/run-or
      ::pcp/run-next
      ::pcp/foreign-ast
      ::pcp/source-for-attrs
      ::pcp/node-parents])
   (pbir/env-table-resolver ::pcr/node-run-stats ::pcp/node-id
     [::pcr/run-duration-ms
      ::pcr/node-run-input
      ::pcr/node-error])])

(def stats-index (pci/register stats-registry))

(defn run-stats-env [stats]
  (-> stats-index
      (assoc :com.wsscode.pathom3.interface.smart-map/keys-mode :com.wsscode.pathom3.interface.smart-map/keys-mode-reachable)
      (merge stats)))
