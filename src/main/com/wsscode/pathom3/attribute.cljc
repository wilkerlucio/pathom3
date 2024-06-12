(ns com.wsscode.pathom3.attribute
  "Core specs of Pathom"
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]))

(>def ::attribute keyword?)
(>def ::parameterized-attribute (s/and seq? (s/cat :attr ::attribute :params (s/? ::params))))
(>def ::attribute-maybe-parameterized (s/or :plain-attr ::attribute :parameterized ::parameterized-attribute))

(>def ::attributes-set (s/coll-of ::attribute :kind set?))
