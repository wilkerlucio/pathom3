(ns com.wsscode.pathom3.cache
  (:require
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]))

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
    (if-let [cached-value (get @cache* cache-key)]
      cached-value
      (let [res (f)]
        (swap! cache* assoc cache-key res)
        res))
    (f)))
