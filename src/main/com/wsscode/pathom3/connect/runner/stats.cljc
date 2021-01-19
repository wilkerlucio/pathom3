(ns com.wsscode.pathom3.connect.runner.stats
  (:require
    [com.wsscode.pathom3.attribute :as p.attr]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]))

; region performance

(defn duration-resolver [attr]
  (let [op-name     (symbol (str (pbir/attr-munge attr) "-duration"))
        start-kw    (keyword (namespace attr) (str (name attr) "-start-ms"))
        finish-kw   (keyword (namespace attr) (str (name attr) "-finish-ms"))
        duration-kw (keyword (namespace attr) (str (name attr) "-duration-ms"))]
    (pco/resolver op-name
      {::pco/input  [start-kw finish-kw]
       ::pco/output [duration-kw]}
      (fn [_ input]
        {duration-kw (- (finish-kw input) (start-kw input))}))))

(pco/defresolver resolver-accumulated-duration
  [{::pcr/keys [node-run-stats]} _]
  {::resolver-accumulated-duration-ms
   (transduce (map #(- (::pcr/resolver-run-finish-ms %)
                       (::pcr/resolver-run-start-ms %))) + 0 (vals node-run-stats))})

(pco/defresolver overhead-duration
  [{::pcr/keys [graph-run-duration-ms]
    ::keys     [resolver-accumulated-duration-ms]}]
  {::overhead-duration-ms
   (- graph-run-duration-ms resolver-accumulated-duration-ms)})

(pco/defresolver overhead-pct
  [{::pcr/keys [graph-run-duration-ms]
    ::keys     [overhead-duration-ms]}]
  {::overhead-duration-percentage
   (double (/ overhead-duration-ms graph-run-duration-ms))})

; endregion

; region errors

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
   attribute-error
   (duration-resolver ::pcr/node-run)
   (duration-resolver ::pcr/resolver-run)
   (duration-resolver ::pcr/batch-run)
   (duration-resolver ::pcr/graph-run)
   (duration-resolver ::pcr/compute-plan-run)
   (pbir/single-attr-with-env-resolver ::p.attr/attribute ::pcp/node-id
     #(get (::pcp/index-attrs %) %2 ::pco/unknown-value))
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
     [::pcr/resolver-run-start-ms
      ::pcr/resolver-run-finish-ms
      ::pcr/batch-run-start-ms
      ::pcr/batch-run-finish-ms
      ::pcr/node-resolver-input
      ::pcr/node-resolver-output
      ::pcr/node-error])])

(def stats-index (pci/register stats-registry))

(defn run-stats-env [stats]
  (-> stats
      (pci/register stats-index)))
