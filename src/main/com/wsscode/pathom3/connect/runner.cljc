(ns com.wsscode.pathom3.connect.runner
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.operation.protocols :as pco.prot]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.entity-tree :as p.ent]))

(>defn all-requires-ready?
  "Check if all requirements from the node are present in the current entity."
  [env {::pcp/keys [requires]}]
  [map? (s/keys :req [::pcp/requires])
   => boolean?]
  (let [entity (p.ent/entity env)]
    (every? #(contains? entity %) (keys requires))))

(declare run-node!)

(defn merge-resolver-response!
  "This function gets the map returned from the resolver and merge the data in the
  current cache-tree."
  [env response]
  (if (map? response)
    (p.ent/swap-entity! env p.ent/merge-entity-data response))
  env)

(defn run-next-node!
  "Runs the next node associated with the node, in case it exists."
  [{::pcp/keys [graph] :as env} {::pcp/keys [run-next]}]
  (if run-next
    (run-node! env (pcp/get-node graph run-next))))

(defn call-resolver-from-node
  "Evaluates a resolver using node information.

  When this function runs the resolver, if filters the data to only include the keys
  mentioned by the resolver input. This is important to ensure that the resolver is
  not using some key that came accidentally due to execution order, that would lead to
  brittle executions."
  [env
   {::pco/keys [op-name]
    ::pcp/keys [input]
    :as        node}]
  (let [input-keys (keys input)
        resolver   (pci/resolver env op-name)
        env        (assoc env ::pcp/node node)
        entity     (p.ent/entity env)]
    (pco.prot/-resolve resolver env (select-keys entity input-keys))))

(defn run-resolver-node!
  "This function evaluates the resolver associated with the node.

  First it checks if the expected results from the resolver are already available. In
  case they are, the resolver call is skipped."
  [env node]
  (if (all-requires-ready? env node)
    (run-next-node! env node)
    (let [response (call-resolver-from-node env node)]
      (if (merge-resolver-response! env response)
        (run-next-node! env node)))))

(>defn run-or-node!
  [{::pcp/keys [graph] :as env} {::pcp/keys [run-or] :as or-node}]
  [(s/keys :req [::pcp/graph]) ::pcp/node => nil?]
  (loop [nodes run-or]
    (let [[node-id & tail] nodes]
      (when node-id
        (run-node! env (pcp/get-node graph node-id))
        (if-not (all-requires-ready? env or-node)
          (recur tail)))))
  (run-next-node! env or-node))

(>defn run-and-node!
  "Given an AND node, runs every attached node, then runs the attached next."
  [{::pcp/keys [graph] :as env} {::pcp/keys [run-and] :as and-node}]
  [(s/keys :req [::pcp/graph]) ::pcp/node => nil?]
  (doseq [node-id run-and]
    (run-node! env (pcp/get-node graph node-id)))
  (run-next-node! env and-node))

(>defn run-node!
  "Run a node from the compute graph. This will start the processing on the sent node
  and them will run everything that's connected to this node as sequences of it.

  The result is going to build up at ::p.ent/cache-tree*, after the run is concluded
  the output will be there.

  TODO: return diagnostic of the running process."
  [env node]
  [(s/keys :req [::pcp/graph ::p.ent/cache-tree*]) ::pcp/node
   => nil?]
  (case (pcp/node-kind node)
    ::pcp/node-resolver
    (run-resolver-node! env node)

    ::pcp/node-and
    (run-and-node! env node)

    ::pcp/node-or
    (run-or-node! env node)

    nil))

(>defn run-graph!
  "Run the root node of the graph."
  [{::pcp/keys [graph] :as env}]
  [(s/keys :req [::pcp/graph ::p.ent/cache-tree*]) => nil?]
  (run-node! env (pcp/get-root-node graph)))
