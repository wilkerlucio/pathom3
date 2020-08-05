(ns com.wsscode.pathom3.entity-tree
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.core :as misc]
    [com.wsscode.pathom3.specs :as p.spec]))

(>def ::entity-tree map?)
(>def ::entity-tree* misc/atom?)

(>def ::cache-tree* ::entity-tree*)
(>def ::output-tree* ::entity-tree*)

(>defn entity
  "Returns the cache entity map at the current path."
  [{::keys [cache-tree*] ::p.spec/keys [path]}]
  [(s/keys :req [::cache-tree* ::p.spec/path])
   => map?]
  (get-in @cache-tree* path {}))

(>defn merge-entity-data
  "Specialized merge versions that work on entity data."
  [entity new-data]
  [::entity-tree ::entity-tree => ::entity-tree]
  (reduce-kv
    assoc
    entity
    new-data))
