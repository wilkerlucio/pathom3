(ns com.wsscode.pathom3.connect.indexes
  (:require [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
            [com.wsscode.pathom3.connect.operation :as pco]))

(>def ::index-oir map?)
(>def ::index-io map?)
(>def ::index-resolvers map?)
(>def ::index-mutations map?)
(>def ::index-attributes map?)

(defn register [indexes operation-or-operations])
