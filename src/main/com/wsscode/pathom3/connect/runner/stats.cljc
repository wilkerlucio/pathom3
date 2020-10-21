(ns com.wsscode.pathom3.connect.runner.stats
  (:require
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]))

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

(def stats-registry
  [resolver-accumulated-duration
   overhead-duration
   overhead-pct
   (pbir/env-table-resolver ::pcp/nodes ::pcp/node-id
                            [::pco/op-name
                             ::pcp/expects
                             ::pcp/input
                             ::pcp/run-and
                             ::pcp/run-or
                             ::pcp/run-next
                             ::pcp/foreign-ast
                             ::pcp/source-for-attrs
                             ::pcp/after-nodes])
   (pbir/env-table-resolver ::pcr/node-run-stats ::pcp/node-id
                            [::pcr/run-duration-ms
                             ::pcr/node-run-input])])

(def stats-index (pci/register stats-registry))

(defn run-stats-env [{::pcp/keys [nodes]
                      ::pcr/keys [node-run-stats]}]
  (-> stats-index
      (assoc
        :com.wsscode.pathom3.interface.smart-map/keys-mode :com.wsscode.pathom3.interface.smart-map/keys-mode-reachable
        ::pcp/nodes nodes
        ::pcr/node-run-stats node-run-stats)))
