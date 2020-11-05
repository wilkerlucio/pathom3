(ns com.wsscode.pathom3.interface.smart-map
  "Smart map is a Pathom interface that provides a map-like data structure, but this
  structure will realize the values as the user requests then. Values realized are
  cached, meaning that subsequent lookups are fast."
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.connect.runner.stats :as pcrs]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [edn-query-language.core :as eql]
    #?(:clj [potemkin.collections :refer [def-map-type]])))

(declare smart-map smart-map?)

(>def ::keys-mode
  #{::keys-mode-cached
    ::keys-mode-reachable})

(>def ::wrap-nested? boolean?)

(>defn with-keys-mode
  "Configure how the Smart Map should respond to `keys`.

  Available modes:

  ::keys-mode-cached (default): in this mode, keys will return only the keys that are
  currently cached in the Smart Map. This is the safest mode, given it will only
  read things from memory.

  ::keys-mode-reachable: in this mode, keys will list all possible keys that Pathom can
  reach given the current indexes and available data. Be careful with this mode, on large
  graphs there is a risk that a scan operation (like printing it) may trigger a large amount of resolver
  processing."
  [env key-mode]
  [map? ::keys-mode => map?]
  (assoc env ::keys-mode key-mode))

(>defn with-wrap-nested?
  "Configure smart map to enable or disable the automatic wrapping of nested structures."
  [env wrap?]
  [map? ::wrap-nested? => map?]
  (assoc env ::wrap-nested? wrap?))

(defn wrap-smart-map
  "If x is a composite data structure, return the data wrapped by smart maps."
  [{::keys [wrap-nested?]
    :or    {wrap-nested? true}
    :as    env} x]
  (cond
    (not wrap-nested?)
    x

    (coll/native-map? x)
    (smart-map env x)

    (sequential? x)
    (if (vector? x)
      (mapv #(wrap-smart-map env %) x)
      (map #(wrap-smart-map env %) x))

    (set? x)
    (into #{} (map #(wrap-smart-map env %)) x)

    :else
    x))

(defn sm-get
  "Get a property from a smart map.

  First it checks if the property is available in the cache-tree, if not it triggers
  the connect engine to lookup for the property. After the lookup is triggered the
  cache-tree will be updated in place, note this has a mutable effect in this data,
  but this change is consistent.

  Repeated lookups will use the cache-tree and should be as fast as reading from a
  regular Clojure map."
  ([env k] (sm-get env k nil))
  ([{::p.ent/keys [entity-tree*] :as env} k default-value]
   (let [ent-tree @entity-tree*]
     (if-let [x (find ent-tree k)]
       (wrap-smart-map env (val x))
       (let [ast {:type     :root
                  :children [{:type :prop, :dispatch-key k, :key k}]}]
         (pcr/run-graph! env ast entity-tree*)
         (wrap-smart-map env (get @entity-tree* k default-value)))))))

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

(defn sm-dissoc
  "Creates a new smart map by adding k v to the initial context.

  When you read information in the smart map, that information is cached into an internal
  atom, and any dependencies that were required to get to the requested data is also
  included in this atom.

  When you assoc into a smart map, it will discard all the data loaded by Pathom itself
  and will be like you have assoced in the original map and created a new smart object."
  [env k]
  (smart-map env
    (-> (::source-context env)
        (dissoc k))))

(defn sm-keys
  "Retrieve the keys in the smart map cache-tree."
  [{::keys [keys-mode] :as env}]
  (case keys-mode
    ::keys-mode-reachable
    (pci/reachable-attributes env (p.ent/entity env))

    (keys (p.ent/entity env))))

(defn sm-contains?
  "Check if a property is present in the cache-tree."
  [env k]
  (let [ks (into #{} (sm-keys env))]
    (contains? ks k)))

(defn sm-meta
  "Returns meta data of smart map, which is the same as the meta data from context
   map used to create the smart map."
  [env]
  (meta (p.ent/entity env)))

(defn sm-with-meta
  "Return a new smart-map with the given meta."
  [env meta]
  (smart-map env (with-meta (p.ent/entity env) meta)))

(defn sm-find
  "Check if attribute can be found in the smart map."
  [env k]
  (if (or (sm-contains? env k)
          (pci/attribute-reachable? env (p.ent/entity env) k))
    (coll/make-map-entry k (sm-get env k))))

; region type definition

#?(:cljs
   (deftype SmartMapEntry [env key]
     Object
     (indexOf [coll x]
              (-indexOf coll x 0))
     (indexOf [coll x start]
              (-indexOf coll x start))
     (lastIndexOf [coll x]
                  (-lastIndexOf coll x (count coll)))
     (lastIndexOf [coll x start]
                  (-lastIndexOf coll x start))

     IMapEntry
     (-key [node] key)
     (-val [node] (sm-get env key))

     IEquiv
     (-equiv [coll other] (equiv-sequential coll other))

     IMeta
     (-meta [node] nil)

     IWithMeta
     (-with-meta [node meta]
                 (with-meta [key (-val node)] meta))

     IStack
     (-peek [node] (-val node))

     (-pop [node] [key])

     ICollection
     (-conj [node o] [key val o])

     IEmptyableCollection
     (-empty [node] nil)

     ISequential
     ISeqable
     (-seq [node] (IndexedSeq. #js [key (-val node)] 0 nil))

     IReversible
     (-rseq [node] (IndexedSeq. #js [(-val node) key] 0 nil))

     ICounted
     (-count [node] 2)

     IIndexed
     (-nth [node n]
           (cond (== n 0) key
                 (== n 1) (-val node)
                 :else (throw (js/Error. "Index out of bounds"))))

     (-nth [node n not-found]
           (cond (== n 0) key
                 (== n 1) (-val node)
                 :else not-found))

     ILookup
     (-lookup [node k] (-nth node k nil))
     (-lookup [node k not-found] (-nth node k not-found))

     IAssociative
     (-assoc [node k v]
             (assoc [key (-val node)] k v))
     (-contains-key? [node k]
                     (or (== k 0) (== k 1)))

     IFind
     (-find [node k]
            (case k
              0 (MapEntry. 0 key nil)
              1 (MapEntry. 1 (-val node) nil)
              nil))

     IVector
     (-assoc-n [node n v]
               (-assoc-n [key (-val node)] n v))

     IReduce
     (-reduce [node f]
              (ci-reduce node f))

     (-reduce [node f start]
              (ci-reduce node f start))

     IFn
     (-invoke [node k]
              (-nth node k))

     (-invoke [node k not-found]
              (-nth node k not-found))))

#?(:clj
   (def-map-type SmartMap [env]
     (get [_ k default-value] (sm-get env k default-value))
     (assoc [_ k v] (sm-assoc env k v))
     (dissoc [_ k] (sm-dissoc env k))
     (keys [_] (sm-keys env))
     (meta [_] (sm-meta env))
     (empty [_] (smart-map env (with-meta {} (sm-meta env))))
     (with-meta [_ new-meta] (sm-with-meta env new-meta))
     (entryAt [_ k] (sm-find env k)))

   :cljs
   (deftype SmartMap [env]
     Object
     (toString [_] (pr-str* (p.ent/entity env)))
     (equiv [_ other] (-equiv (p.ent/entity env) other))

     ;; ES6
     (keys [_] (es6-iterator (sm-keys env)))
     (entries [_] (es6-entries-iterator (seq (p.ent/entity env))))
     (values [_] (es6-iterator (vals (p.ent/entity env))))
     (has [_ k] (sm-contains? env k))
     (get [_ k not-found] (-lookup (p.ent/entity env) k not-found))
     (forEach [_ f] (doseq [[k v] (p.ent/entity env)] (f v k)))

     ICloneable
     (-clone [_] (smart-map env (p.ent/entity env)))

     IWithMeta
     (-with-meta [_ new-meta] (sm-with-meta env new-meta))

     IMeta
     (-meta [_] (sm-meta env))

     ICollection
     (-conj [coll entry]
            (if (vector? entry)
              (-assoc coll (-nth entry 0) (-nth entry 1))
              (loop [ret coll
                     es  (seq entry)]
                (if (nil? es)
                  ret
                  (let [e (first es)]
                    (if (vector? e)
                      (recur (-assoc ret (-nth e 0) (-nth e 1))
                        (next es))
                      (throw (js/Error. "conj on a map takes map entries or seqables of map entries"))))))))

     IEmptyableCollection
     (-empty [_] (-with-meta (smart-map env {}) meta))

     IEquiv
     (-equiv [_ other] (-equiv (p.ent/entity env) other))

     IHash
     (-hash [_] (hash (p.ent/entity env)))

     ISeqable
     (-seq [_]
           (some->> (seq (sm-keys env))
                    (map #(SmartMapEntry. env %))))

     ICounted
     (-count [_] (count (p.ent/entity env)))

     ILookup
     (-lookup [_ k] (sm-get env k nil))
     (-lookup [_ k not-found] (sm-get env k not-found))

     IAssociative
     (-assoc [_ k v] (sm-assoc env k v))
     (-contains-key? [_ k] (sm-contains? env k))

     IFind
     (-find [_ k] (sm-find env k))

     IMap
     (-dissoc [_ k] (sm-dissoc env k))

     IKVReduce
     (-kv-reduce [_ f init]
                 (reduce-kv (fn [cur k v] (f cur k (wrap-smart-map env v))) init (p.ent/entity env)))

     IIterable
     (-iterator [this]
                (transformer-iterator (map #(SmartMapEntry. env %))
                                      (-iterator (sm-keys env)) false))

     IReduce
     (-reduce [coll f] (iter-reduce coll f))
     (-reduce [coll f start] (iter-reduce coll f start))

     IFn
     (-invoke [_ k] (sm-get env k))
     (-invoke [_ k not-found] (sm-get env k not-found))

     IPrintWithWriter
     (-pr-writer [_ writer opts]
                 (-pr-writer (p.ent/entity env) writer opts))))

(defn smart-map? [x] (instance? SmartMap x))

(>def ::smart-map smart-map?)

; endregion

(>defn sm-env
  "Extract the env map from the smart map."
  [^SmartMap smart-map]
  [::smart-map => map?]
  (.-env smart-map))

(>defn sm-update-env
  "Update smart map environment"
  [^SmartMap sm f & args]
  [::smart-map fn? (s/* any?) => ::smart-map]
  (let [env (apply f (sm-env sm) args)]
    (smart-map env (::source-context env))))

(>defn sm-get-with-stats
  "Return the graph run analysis, use for debugging. You can find the get value return
  in the ::psm/value key.

  Note that if the value is cached, there will be a blank stats."
  ([^SmartMap sm k]
   [::smart-map any? => any?]
   (let [{::p.ent/keys [entity-tree*] :as env} (sm-env sm)
         ast       {:type     :root
                    :children [{:type :prop, :dispatch-key k, :key k}]}
         run-stats (pcr/run-graph! env ast entity-tree*)]
     (smart-map
       (pcrs/run-stats-env run-stats)
       (-> run-stats
           (dissoc ::pcr/node-run-stats)
           (assoc ::value (wrap-smart-map env (get @entity-tree* k))))))))

(>defn sm-assoc!
  "Assoc on the smart map in place, this function mutates the current cache and return
  the same instance of smart map.

  You should use this only in cases where the optimization is required, try starting
  with the immutable versions first, given this has side effects and so more error phone."
  [^SmartMap smart-map k v]
  [::smart-map any? any? => ::smart-map]
  (swap! (-> smart-map sm-env ::p.ent/entity-tree*) assoc k v)
  smart-map)

(>defn sm-dissoc!
  "Dissoc on the smart map in place, this function mutates the current cache and return
  the same instance of smart map.

  You should use this only in cases where the optimization is required, try starting
  with the immutable versions first, given this has side effects and so more error phone."
  [^SmartMap smart-map k]
  [::smart-map any? => ::smart-map]
  (swap! (-> smart-map sm-env ::p.ent/entity-tree*) dissoc k)
  smart-map)

(>defn sm-touch-ast!
  [^SmartMap smart-map ast]
  [::smart-map :edn-query-language.ast/node
   => ::smart-map]
  (let [env (sm-env smart-map)]
    (pcr/run-graph! env ast (::p.ent/entity-tree* env))
    smart-map))

(>defn sm-touch!
  "Will pre-fetch data in a smart map, given the EQL request. Use this to optimize the
  load of data ahead of time, instead of pulling one by one lazily."
  [^SmartMap smart-map eql]
  [::smart-map ::eql/query
   => ::smart-map]
  (sm-touch-ast! smart-map (eql/query->ast eql)))

(>defn ^SmartMap smart-map
  "Create a new smart map.

  Smart maps are a special data structure that realizes properties using Pathom resolvers.

  They work like maps and can be used interchangeable with it.

  To create a smart map you need send an environment with the indexes and a context
  map with the initial context data:

      (smart-map (pci/register [resolver1 resolver2]) {:some \"context\"})

  When the value of a property of the smart map is a map, that map will also be cast
  into a smart map, including maps inside collections."
  ([env]
   [(s/keys :opt [::pci/index-oir]) => ::smart-map]
   (smart-map env {}))
  ([env context]
   [(s/keys :opt [::pci/index-oir])
    map? => ::smart-map]
   (->SmartMap
     (-> env
         (p.ent/with-entity context)
         (assoc ::source-context context)))))
