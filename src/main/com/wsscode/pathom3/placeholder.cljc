(ns com.wsscode.pathom3.placeholder
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.pathom3.specs :as p.spec]))

(>def ::placeholder-prefixes (s/coll-of string? :kind set?))

(>defn placeholder-key?
  "Check if a given key is a placeholder."
  [{::p.spec/keys [placeholder-prefixes]} k]
  [(s/keys :opt [::placeholder-prefixes]) any?
   => boolean?]
  (let [placeholder-prefixes (or placeholder-prefixes #{">"})]
    (and (keyword? k)
         (contains? placeholder-prefixes (namespace k)))))

(>defn find-closest-non-placeholder-parent-join-key
  "Find the closest parent key that's not a placeholder key."
  [{::p.spec/keys [path] :as env}]
  [(s/keys :opt [::p.spec/path])
   => (? ::p.spec/path-entry)]
  (->> (or path []) rseq (drop 1) (remove #(placeholder-key? env %)) first))
