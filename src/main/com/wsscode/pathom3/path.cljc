(ns com.wsscode.pathom3.path
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.attribute :as p.attr]
    [edn-query-language.core :as eql]))

(>def ::path-entry
  (s/or :attr ::p.attr/attribute
        :ident ::eql/ident
        :index nat-int?
        :call symbol?))

(>def ::path (s/coll-of ::path-entry :kind vector?))

(>defn append-path
  [env path-entry]
  [(s/keys :req [::path]) ::path-entry
   => map?]
  (update env ::path coll/vconj path-entry))

(>defn root?
  "Check if current path is the root, meaning a blank path."
  [{::keys [path]}]
  [(s/keys :opt [::path]) => boolean?]
  (empty? path))
