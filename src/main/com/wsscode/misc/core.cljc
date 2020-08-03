(ns com.wsscode.misc.core
  (:require
    [clojure.set :as set])
  #?(:clj
     (:import
       (clojure.lang
         IDeref)
       (java.util
         UUID))))

(defn atom? [x]
  #?(:clj  (instance? IDeref x)
     :cljs (satisfies? IDeref x)))

(defn cljc-random-uuid []
  #?(:clj  (UUID/randomUUID)
     :cljs (random-uuid)))

(defn distinct-by
  "Returns a lazy sequence of the elements of coll, removing any elements that
  return duplicate values when passed to a function f."
  ([f]
   (fn [rf]
     (let [seen (volatile! #{})]
       (fn
         ([] (rf))
         ([result] (rf result))
         ([result x]
          (let [fx (f x)]
            (if (contains? @seen fx)
              result
              (do (vswap! seen conj fx)
                  (rf result x)))))))))
  ([f coll]
   (let [step (fn step [xs seen]
                (lazy-seq
                  ((fn [[x :as xs] seen]
                     (when-let [s (seq xs)]
                       (let [fx (f x)]
                         (if (contains? seen fx)
                           (recur (rest s) seen)
                           (cons x (step (rest s) (conj seen fx)))))))
                   xs seen)))]
     (step coll #{}))))

(defn dedupe-by
  "Returns a lazy sequence removing consecutive duplicates in coll when passed to a function f.
  Returns a transducer when no collection is provided."
  {:added "1.7"}
  ([f]
   (fn [rf]
     (let [pv (volatile! ::none)]
       (fn
         ([] (rf))
         ([result] (rf result))
         ([result x]
          (let [prior @pv
                fx    (f x)]
            (vreset! pv fx)
            (if (= prior fx)
              result
              (rf result x))))))))
  ([f coll] (sequence (dedupe-by f) coll)))

(defn index-by
  "Like group by, but will keep only the last result."
  [f coll]
  (reduce
    (fn [m x]
      (assoc m (f x) x))
    {}
    coll))

(def sconj (fnil conj #{}))
(def vconj (fnil conj []))

(defn queue
  "Return an immutable queue or create one from coll."
  ([] #?(:clj  clojure.lang.PersistentQueue/EMPTY
         :cljs cljs.core/PersistentQueue.EMPTY))
  ([coll]
   (into (queue) coll)))

(defn map-keys
  "Map over the given hash-map keys.

  Example:
    (map-keys #(str/replace (name %) \"_\" \"-\") {\"foo_bar\" 1}) => {\"foo-bar\" 1}
  "
  [f m]
  (into {} (for [[k v] m] [(f k) v])))

(defn map-vals
  "Map over the given hash-map vals.

  Example:
    (map-vals inc {:a 1 :b 2})
  "
  [f m]
  (into {} (for [[k v] m] [k (f v)])))

(defn merge-grow
  "Additive merging.

  When merging maps, it does a deep merge.
  When merging sets, makes a union of them.

  When value of the right side is nil, the left side will be kept.

  For the rest works as standard merge."
  ([] {})
  ([a] a)
  ([a b]
   (cond
     (and (set? a) (set? b))
     (set/union a b)

     (and (map? a) (map? b))
     (merge-with merge-grow a b)

     (nil? b) a

     :else
     b)))
