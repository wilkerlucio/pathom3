(ns com.wsscode.pathom3.connect.runner.stats
  (:require
    [com.wsscode.misc.core :as misc]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]))

(pco/defresolver resolver-accumulated-duration
  [{::pcr/keys [node-run-stats]}]
  ::pcr/resolver-accumulated-duration-ns
  (transduce (map ::pcr/run-duration-ns) + 0 (vals node-run-stats)))

(pco/defresolver overhead-duration
  [{::pcr/keys [graph-process-duration-ns
                resolver-accumulated-duration-ns]}]
  ::pcr/overhead-duration-ns
  (- graph-process-duration-ns resolver-accumulated-duration-ns))

(pco/defresolver overhead-pct
  [{::pcr/keys [graph-process-duration-ns
                overhead-duration-ns]}]
  ::pcr/overhead-duration-percentage
  (double (/ overhead-duration-ns graph-process-duration-ns)))

(defn duration-extensions [attr]
  (let [ns (namespace attr)
        n  (name attr)
        mk #(keyword ns (str n "-" %))]
    [(pbir/single-attr-resolver (mk "ns") (mk "ms") #(misc/round (/ % 1000000)))
     (pbir/single-attr-resolver (mk "ms") (mk "s") #(misc/round (/ % 1000)))
     (pbir/single-attr-resolver (mk "s") (mk "mins") #(misc/round (/ % 60)))
     (pbir/single-attr-resolver (mk "mins") (mk "hours") #(misc/round (/ % 60)))]))

(def stats-registry
  [resolver-accumulated-duration
   overhead-duration
   overhead-pct
   (duration-extensions ::pcr/graph-process-duration)
   (duration-extensions ::pcr/run-duration)
   (duration-extensions ::pcr/resolver-accumulated-duration)
   (duration-extensions ::pcr/overhead-duration)
   (pbir/attribute-table-resolver ::pcp/nodes ::pcp/node-id
                                  [::pco/op-name
                                   ::pcp/requires
                                   ::pcp/input
                                   ::pcp/run-and
                                   ::pcp/run-or
                                   ::pcp/run-next
                                   ::pcp/foreign-ast
                                   ::pcp/source-for-attrs
                                   ::pcp/after-nodes])
   (pbir/attribute-table-resolver ::pcr/node-run-stats ::pcp/node-id
                                  [::pcr/run-duration-ns
                                   ::pcr/node-run-input])])

(def stats-index (pci/register stats-registry))
