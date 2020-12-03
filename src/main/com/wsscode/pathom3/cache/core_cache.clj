(ns com.wsscode.pathom3.cache.core-cache
  (:require
    [clojure.core.cache.wrapped :as cache]
    [com.wsscode.pathom3.cache :as p.cache]))

(defrecord CoreCache [cache*]
  p.cache/CacheStore
  (-cache-lookup-or-miss [_ cache-key f]
                         (cache/lookup-or-miss cache* cache-key f)))

(defn lru-cache [initial max-elements]
  (->CoreCache (cache/lru-cache-factory initial :threshold max-elements)))
