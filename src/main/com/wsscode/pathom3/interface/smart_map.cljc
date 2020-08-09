(ns com.wsscode.pathom3.interface.smart-map
  "Smart map is a Pathom interface that provides a map-like data structure, but this
  structure will realize the values as the user requests then. Values realized are
  cached, meaning that subsequent lookups are fast."
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.pathom3.connect.planner :as pcp]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.shape-descriptor :as pfsd]
    #?(:clj [potemkin.collections :refer [def-map-type]])))

(declare smart-map)

(defn wrap-smart-map
  "If x is a composite data structure, return the data wrapped by smart maps."
  [env x]
  (cond
    (map? x)
    (smart-map env x)

    (sequential? x)
    (map #(wrap-smart-map env %) x)

    (set? x)
    (into #{} (map #(wrap-smart-map env %)) x)

    :else
    x))

(defn sm-get
  [{::p.ent/keys [cache-tree*] :as env} k default-value]
  (let [cache-tree @cache-tree*]
    (if (contains? cache-tree k)
      (wrap-smart-map env (get cache-tree k))
      (let [ast   {:type     :root
                   :children [{:type :prop, :dispatch-key k, :key k}]}
            graph (pcp/compute-run-graph
                    (assoc env
                      ::pcp/available-data (pfsd/data->shape-descriptor cache-tree)
                      :edn-query-language.ast/node ast))]
        (pcr/run-graph! (assoc env ::pcp/graph graph))
        (wrap-smart-map env (get @cache-tree* k default-value))))))

(defn sm-assoc
  "Creates a new smart map by adding k v to the initial context.

  When you read information in the smart map, that information is cached into an internal
  atom, and any dependencies that were required to get to the requested data is also
  included in this atom.

  When you assoc into a smart map, it will discard all the data loaded by Pathom itself
  and will be like you have assoced in the original map and created a new smart object."
  [env k v]
  (smart-map env
    (-> (::source-context env)
        (assoc k v))))

(defn sm-dissoc [env k]
  (smart-map env
    (-> (::source-context env)
        (dissoc k))))

(defn sm-keys [env]
  (keys (p.ent/cache-tree env)))

(defn sm-contains? [env k]
  (contains? (p.ent/cache-tree env) k))

(defn sm-meta
  "Returns meta data of smart map, which is the same as the meta data from context
   map used to create the smart map."
  [env]
  (meta (p.ent/cache-tree env)))

(defn sm-with-meta [env meta]
  (smart-map env (with-meta (p.ent/cache-tree env) meta)))

#?(:clj
   (def-map-type SmartMap [env]
     (get [_ k default-value] (sm-get env k default-value))
     (assoc [_ k v] (sm-assoc env k v))
     (dissoc [_ k] (sm-dissoc env k))
     (keys [_] (sm-keys env))
     (meta [_] (sm-meta env))
     (with-meta [_ new-meta] (sm-with-meta env new-meta)))

   :cljs
   (deftype SmartMap [env]
     Object
     (toString [_] (pr-str* (p.ent/cache-tree env)))
     (equiv [_ other] (-equiv (p.ent/cache-tree env) other))

     ;; EXPERIMENTAL: subject to change
     (keys [_] (es6-iterator (keys (p.ent/cache-tree env))))
     (entries [_] (es6-entries-iterator (seq (p.ent/cache-tree env))))
     (values [_] (es6-iterator (vals (p.ent/cache-tree env))))
     (has [_ k] (sm-contains? env k))
     (get [_ k not-found] (-lookup (p.ent/cache-tree env) k not-found))
     (forEach [_ f] (doseq [[k v] (p.ent/cache-tree env)] (f v k)))

     ICloneable
     (-clone [_] (smart-map env (p.ent/cache-tree env)))

     IWithMeta
     (-with-meta [_ new-meta] (sm-with-meta env new-meta))

     IMeta
     (-meta [_] (sm-meta env))

     ICollection
     (-conj [coll entry]
            (if (vector? entry)
              (-assoc coll (-nth entry 0) (-nth entry 1))
              (loop [ret coll es (seq entry)]
                (if (nil? es)
                  ret
                  (let [e (first es)]
                    (if (vector? e)
                      (recur (-assoc ret (-nth e 0) (-nth e 1))
                        (next es))
                      (throw (js/Error. "conj on a map takes map entries or seqables of map entries"))))))))

     IEmptyableCollection
     (-empty [coll] (-with-meta (smart-map env {}) meta))

     IEquiv
     (-equiv [coll other] (-equiv (p.ent/cache-tree env) other))

     IHash
     (-hash [coll] (hash (p.ent/cache-tree env)))

     IIterable
     (-iterator [this] (-iterator (p.ent/cache-tree env)))

     ISeqable
     (-seq [coll] (-seq (p.ent/cache-tree env)))

     ICounted
     (-count [coll] (count (p.ent/cache-tree env)))

     ILookup
     (-lookup [_ k] (sm-get env k nil))
     (-lookup [_ k not-found] (sm-get env k not-found))

     IAssociative
     (-assoc [_ k v] (sm-assoc env k v))
     (-contains-key? [_ k] (sm-contains? env k))

     #_ #_
     IFind
     (-find [coll k]
       (let [idx (array-map-index-of coll k)]
         (when-not (== idx -1)
           (MapEntry. (aget arr idx) (aget arr (inc idx)) nil))))

     IMap
     (-dissoc [coll k] (sm-dissoc env k))

     #_ #_
     IKVReduce
     (-kv-reduce [coll f init]
       (let [len (alength arr)]
         (loop [i 0 init init]
           (if (< i len)
             (let [init (f init (aget arr i) (aget arr (inc i)))]
               (if (reduced? init)
                 @init
                 (recur (+ i 2) init)))
             init))))

     #_ #_ #_
     IReduce
     (-reduce [coll f]
       (iter-reduce coll f))
     (-reduce [coll f start]
       (iter-reduce coll f start))

     IFn
     (-invoke [coll k] (-lookup coll k))
     (-invoke [coll k not-found] (-lookup coll k not-found))

     #_ #_
     IEditableCollection
     (-as-transient [coll]
       (TransientArrayMap. (js-obj) (alength arr) (aclone arr)))))

(>def ::smart-map #(instance? SmartMap %))

(>defn sm-env
  "Extract the env map from the smart map."
  [smart-map]
  [::smart-map => map?]
  (.-env smart-map))

(defn sm-assoc!
  "Assoc on the smart map in place, this function mutates the current cache and return
  the same instance of smart map.

  You should use this only in cases where the optimization is required, try starting
  with the immutable versions first, given this has side effects and so more error phone."
  [smart-map k v]
  (swap! (-> smart-map sm-env ::p.ent/cache-tree*) assoc k v)
  smart-map)

(defn sm-dissoc! [smart-map k]
  (swap! (-> smart-map sm-env ::p.ent/cache-tree*) dissoc k)
  smart-map)

(>defn smart-map
  [env context]
  [(s/keys :req [:com.wsscode.pathom3.connect.indexes/index-oir])
   map? => ::smart-map]
  (->SmartMap
    (-> env
        (p.ent/with-cache-tree context)
        (assoc ::source-context context))))
