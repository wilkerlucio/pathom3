(ns com.wsscode.pathom3.cache
  (:require
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]])
  #?(:clj
     (:import
       (clojure.lang
         Atom
         Volatile))))

(defprotocol CacheStore
  (-cache-lookup-or-miss [this cache-key f])
  (-cache-find [this cache-key]
    "Implement a way to read a cache key from the cache. If there is a hit, you must
    return a map entry for the result, otherwise return nil. The map-entry can make
    the distinction between a miss (nil return) vs a value with a miss (a map-entry with
    a value of nil)"))

(extend-protocol CacheStore
  Atom
  (-cache-lookup-or-miss [this cache-key f]
    (let [cache @this]
      (if-let [entry (find cache cache-key)]
        (val entry)
        (let [res (f)]
          (swap! this assoc cache-key res)
          res))))

  (-cache-find [this cache-key]
    (find @this cache-key))

  Volatile
  (-cache-lookup-or-miss [this cache-key f]
    (let [cache @this]
      (if-let [entry (find cache cache-key)]
        (val entry)
        (let [res (f)]
          (vswap! this assoc cache-key res)
          res))))

  (-cache-find [this cache-key]
    (find @this cache-key)))

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

(>defn cache-find
  "Read from cache, without trying to set."
  [cache cache-key]
  [cache-store? any? => (? map-entry?)]
  (-cache-find cache cache-key))
