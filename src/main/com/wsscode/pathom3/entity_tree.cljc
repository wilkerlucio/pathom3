(ns com.wsscode.pathom3.entity-tree
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.core :as misc]
    [com.wsscode.pathom3.specs :as p.spec]))

(>def ::entity-tree map?)
(>def ::entity-tree* misc/atom?)

(>def ::cache-tree* ::entity-tree*)
(>def ::output-tree* ::entity-tree*)

(>defn entity
  "Returns the cache entity map at the current path."
  [{::keys [cache-tree*] ::p.spec/keys [path]}]
  [(s/keys :req [::cache-tree*] :opt [::p.spec/path])
   => map?]
  (get-in @cache-tree* path {}))

(>defn with-cache-tree
  "Set the cache in the environment. Note in this function you must send the cache-tree
  as a map, not as an atom."
  [env cache-tree]
  [map? map? => map?]
  (assoc env ::cache-tree* (atom cache-tree)))

(defn- swap-entity*
  ([cache-tree path f]
   (if (seq path)
     (update-in cache-tree path f)
     (f cache-tree)))
  ([cache-tree path f x]
   (if (seq path)
     (update-in cache-tree path f x)
     (f cache-tree x)))
  ([cache-tree path f x y]
   (if (seq path)
     (update-in cache-tree path f x y)
     (f cache-tree x y)))
  ([cache-tree path f x y & args]
   (if (seq path)
     (apply update-in cache-tree path f x y args)
     (apply f cache-tree x y args))))

(>defn swap-entity!
  "Swap cache-tree at the current path. Returns the updated whole cache-tree."
  ([{::keys [cache-tree*] ::p.spec/keys [path]} f]
   [(s/keys :req [::cache-tree*] :opt [::p.spec/path]) fn?
    => map?]
   (swap! cache-tree* swap-entity* path f))
  ([{::keys [cache-tree*] ::p.spec/keys [path]} f x]
   [(s/keys :req [::cache-tree*] :opt [::p.spec/path]) fn? any?
    => map?]
   (swap! cache-tree* swap-entity* path f x))
  ([{::keys [cache-tree*] ::p.spec/keys [path]} f x y]
   [(s/keys :req [::cache-tree*] :opt [::p.spec/path]) fn? any? any?
    => map?]
   (swap! cache-tree* swap-entity* path f x y))
  ([{::keys [cache-tree*] ::p.spec/keys [path]} f x y & args]
   [(s/keys :req [::cache-tree*] :opt [::p.spec/path]) fn? any? any? (s/+ any?)
    => map?]
   (apply swap! cache-tree* swap-entity* path f x y args)))

(>defn merge-entity-data
  "Specialized merge versions that work on entity data."
  [entity new-data]
  [::entity-tree ::entity-tree => ::entity-tree]
  (reduce-kv
    assoc
    entity
    new-data))
