(ns com.wsscode.pathom3.error
  (:require
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.core :as misc]))

(>def ::errors map?)
(>def ::errors* misc/atom?)
