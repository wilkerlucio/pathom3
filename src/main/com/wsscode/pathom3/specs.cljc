(ns com.wsscode.pathom3.specs
  "Core specs of Pathom"
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]))

(>def ::attribute keyword?)
(>def ::attributes-set (s/coll-of ::attribute :kind set?))
