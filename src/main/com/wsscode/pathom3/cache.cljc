(ns com.wsscode.pathom3.cache
  (:require
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]])
  #?(:clj
     (:import
       (clojure.lang
         Atom
         Volatile))))

(defprotocol CacheStore
  (-cache-lookup-or-miss [this cache-key f]))

(extend-protocol CacheStore
  Atom
  (-cache-lookup-or-miss [this cache-key f]
    (let [cache @this]
      (if-let [entry (find cache cache-key)]
        (val entry)
        (let [res (f)]
          (swap! this assoc cache-key res)
          res))))

  Volatile
  (-cache-lookup-or-miss [this cache-key f]
    (let [cache @this]
      (if-let [entry (find cache cache-key)]
        (val entry)
        (let [res (f)]
          (vswap! this assoc cache-key res)
          res)))))

(defn cache-store? [x] (satisfies? CacheStore x))

(>defn cached
  "Try to read some value from a cache, otherwise run and cache it.

  cache-container is a keyword for the cache container name, consider that the environment
  has multiple cache atoms. If the cache-container key is not present in the env, the
  cache will be ignored and will always run f.

  cache-key is how you decide, in that cache container, what key should be used for
  this cache try.

  f needs to a function of zero arguments.

  Example:

      (cached ::my-cache {::my-cache (atom {})} [3 :foo]
        (fn [] (run-expensive-operation)))"
  [cache-container env cache-key f]
  [keyword? map? any? fn? => any?]
  (if-let [cache* (get env cache-container)]
    (-cache-lookup-or-miss cache* cache-key f)
    (f)))
