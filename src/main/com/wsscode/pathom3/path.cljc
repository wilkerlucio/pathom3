(ns com.wsscode.pathom3.path
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [edn-query-language.core :as eql]))

(>def ::path-entry
  (s/or :attr ::attribute
        :ident ::eql/ident
        :index nat-int?))

(>def ::path (s/coll-of ::path-entry :kind vector?))

(>defn append-path
  [env path-entry]
  [(s/keys :req [::path]) ::path-entry
   => map?]
  (update env ::path conj path-entry))
