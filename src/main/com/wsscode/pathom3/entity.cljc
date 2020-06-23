(ns com.wsscode.pathom3.entity
  (:require [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]))

(>defn merge-entity-data
  "Specialized merge versions that work on entity data."
  [entity new-data]
  [map? map? => map?]
  (merge entity new-data))
