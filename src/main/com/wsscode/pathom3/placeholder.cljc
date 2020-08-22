(ns com.wsscode.pathom3.placeholder
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.pathom3.path :as p.path]))

(>def ::placeholder-prefixes (s/coll-of string? :kind set?))

(>defn placeholder-key?
  "Check if a given key is a placeholder."
  [{::p.path/keys [placeholder-prefixes]} k]
  [(s/keys :opt [::placeholder-prefixes]) any?
   => boolean?]
  (let [placeholder-prefixes (or placeholder-prefixes #{">"})]
    (and (keyword? k)
         (contains? placeholder-prefixes (namespace k)))))

(>defn find-closest-non-placeholder-parent-join-key
  "Find the closest parent key that's not a placeholder key."
  [{::p.path/keys [path] :as env}]
  [(s/keys :opt [::p.path/path])
   => (? ::p.path/path-entry)]
  (->> (or path []) rseq (drop 1) (remove #(placeholder-key? env %)) first))
