(ns com.wsscode.pathom3.entity-tree
  (:require
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.core :as w.misc]))

(>def ::entity-tree map?)
(>def ::entity-tree* w.misc/atom?)

(>def ::cache-tree* ::entity-tree*)
(>def ::output-tree* ::entity-tree*)

(>defn merge-entity-data
  "Specialized merge versions that work on entity data."
  [entity new-data]
  [::entity-tree ::entity-tree => ::entity-tree]
  (reduce-kv
    assoc
    entity
    new-data))
