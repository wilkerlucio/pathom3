(ns com.wsscode.pathom3.error
  (:require
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.refs :as refs]))

(>def ::errors map?)
(>def ::errors* refs/atom?)
