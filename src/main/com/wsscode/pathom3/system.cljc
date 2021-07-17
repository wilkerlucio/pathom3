(ns com.wsscode.pathom3.system
  (:require
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]))

(>def :pathom/lenient-mode?
  "Lenient mode indicates to Pathom that fails should be tolerated. This means Pathom will
  catch all errors and return any data it can in the process. This is in contrast with
  the default strict mode, which fails if any part of the request is unsuccessful."
  boolean?)
