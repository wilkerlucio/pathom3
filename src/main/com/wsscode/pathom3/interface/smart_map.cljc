(ns com.wsscode.pathom3.interface.smart-map
  "Smart map is a Pathom interface that provides a map-like data structure, but this
  structure will realize the values as the user requests then. Values realized are
  cached, meaning that subsequent lookups are fast."
  (:require
    [clojure.core.protocols :as d]
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [<- => >def >defn >fdef ? |]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.misc.refs :as refs]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.connect.runner.stats :as pcrs]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.interface.eql :as p.eql]
    [edn-query-language.core :as eql]
    #?@(:bb  []
        :clj [[potemkin.collections :refer [def-map-type]]])))

(declare smart-map smart-map? sm-env datafy-smart-map)

(>def ::error-mode
  #{::error-mode-silent
    ::error-mode-loud})

(>def ::keys-mode
  "How "
  #{::keys-mode-cached
    ::keys-mode-reachable})

(>def ::wrap-nested?
  "Flag to decide if values read from a Smart Map should also be wrapped on a Smart Map.
  True by default."
  boolean?)

(>def ::smart-map? boolean?)

(>defn with-error-mode
  "This configures what happens when some error occur during the Pathom process to
  provide more data to the Smart Map.

  Available modes:

  ::error-mode-silent (default): Return `nil` as the value, don't throw errors.
  ::error-mode-loud: Throw the errors on read."
  [env error-mode]
  [map? ::error-mode => map?]
  (assoc env ::error-mode error-mode))

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

    ; else case covers already smart map, and other special records
    :else
    x))

(defn smart-run-stats
  "Wrap runner run stats with smart definitions to traverse the runner data."
  [run-stats]
  (smart-map
    (pcrs/run-stats-env run-stats)
    run-stats))

(defn sm-env-get
  "Get a property from a smart map.

  First it checks if the property is available in the cache-tree, if not it triggers
  the connect engine to lookup for the property. After the lookup is triggered the
  cache-tree will be updated in place, note this has a mutable effect in this data,
  but this change is consistent.

  Repeated lookups will use the cache-tree and should be as fast as reading from a
  regular Clojure map."
  ([env k] (sm-env-get env k nil))
  ([{::p.ent/keys [entity-tree*] :as env} k default-value]
   (let [ent-tree @entity-tree*]
     (if-let [x (find ent-tree k)]
       (wrap-smart-map env (val x))
       (let [ast   {:type     :root
                    :children [{:type :prop, :dispatch-key k, :key k}]}
             stats (-> (pcr/run-graph! env ast entity-tree*) meta ::pcr/run-stats)]
         (when-let [error (and (refs/kw-identical? (get env ::error-mode) ::error-mode-loud)
                               (-> (p.eql/process (pcrs/run-stats-env stats)
                                                  {:com.wsscode.pathom3.attribute/attribute k}
                                                  [::pcrs/attribute-error])
                                   ::pcrs/attribute-error
                                   ::pcr/node-error))]
           (throw error))
         (wrap-smart-map env (get @entity-tree* k default-value)))))))

(defn sm-env-assoc
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

(defn sm-env-dissoc
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

(defn sm-env-keys
  "Retrieve the keys in the smart map cache-tree."
  [{::keys [keys-mode] :as env}]
  (case keys-mode
    ::keys-mode-reachable
    (pci/reachable-attributes env (p.ent/entity env))

    (keys (p.ent/entity env))))

(defn sm-env-contains?
  "Check if a property is present in the cache-tree."
  [env k]
  (let [ks (into #{} (sm-env-keys env))]
    (contains? ks k)))

(defn sm-env-meta
  "Returns meta data of smart map, which is the same as the meta data from context
   map used to create the smart map."
  [env]
  (meta (p.ent/entity env)))

(defn sm-env-with-meta
  "Return a new smart-map with the given meta."
  [env meta]
  (smart-map env (with-meta (p.ent/entity env) meta)))

(defn sm-env-find
  "Check if attribute can be found in the smart map."
  [env k]
  (if (or (sm-env-contains? env k)
          (pci/attribute-reachable? env (p.ent/entity env) k))
    (coll/make-map-entry k (sm-env-get env k))))

(defn sm-env-empty
  "Return a new smart map with the same environment and an empty map context."
  [env]
  (smart-map env (with-meta {} (sm-env-meta env))))

(defn sm-env-to-string [env]
  (str "#SmartMap " (p.ent/entity env)))

(defn associative-conj [m entry]
  #?(:cljs
     (if (vector? entry)
       (-assoc m (-nth entry 0) (-nth entry 1))
       (loop [ret m
              es  (seq entry)]
         (if (nil? es)
           ret
           (let [e (first es)]
             (if (vector? e)
               (recur (-assoc ret (-nth e 0) (-nth e 1))
                 (next es))
               (throw (js/Error. "conj on a map takes map entries or seqables of map entries")))))))

     :default
     (if (vector? entry)
       (assoc m (nth entry 0) (nth entry 1))
       (loop [ret m
              es  (seq entry)]
         (if (nil? es)
           ret
           (let [e (first es)]
             (if (vector? e)
               (recur (assoc ret (nth e 0) (nth e 1))
                 (next es))
               (throw (ex-info "conj on a map takes map entries or seqables of map entries" {})))))))))

; region type definition

#?(:bb
   (defn ->LazyMapEntry
     [key_ val_]
     (proxy [clojure.lang.AMapEntry] []
       (key [] key_)
       (val [] (force val_))
       (getKey [] key_)
       (getValue [] (force val_))))

   :cljs
   #_{:clj-kondo/ignore [:private-call]}
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
     (-key [_node] key)
     (-val [_node] (sm-env-get env key))

     IEquiv
     (-equiv [coll other] (equiv-sequential coll other))

     IMeta
     (-meta [_node] nil)

     IWithMeta
     (-with-meta [node meta]
                 (with-meta [key (-val node)] meta))

     IStack
     (-peek [node] (-val node))

     (-pop [_node] [key])

     ICollection
     (-conj [node o] [key (-val node) o])

     IEmptyableCollection
     (-empty [_node] nil)

     ISequential
     ISeqable
     (-seq [node] (IndexedSeq. #js [key (-val node)] 0 nil))

     IReversible
     (-rseq [node] (IndexedSeq. #js [(-val node) key] 0 nil))

     ICounted
     (-count [_node] 2)

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
     (-contains-key? [_node k]
                     (or (== k 0) (== k 1)))

     IFind
     (-find [node k]
            (case k
              0 (coll/make-map-entry 0 key)
              1 (coll/make-map-entry 1 (-val node))
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

#?(:bb (defprotocol ISmartMap (-smart-map-env [this])))

#?(:bb (defn sm-env-lazy-map-entry [env k]
         (->LazyMapEntry k (delay (sm-env-get env k)))))

#?(:bb
   (defn ->SmartMap [env]
     (proxy [clojure.lang.APersistentMap clojure.lang.IMeta clojure.lang.IObj]
            []
       (valAt
         ([k]
          (case k
            ::env
            env

            (sm-env-get env k)))
         ([k default-value]
          (case k
            ::env
            env

            (sm-env-get env k default-value))))
       (iterator []
         (coll/iterator
           (eduction
             (map #(sm-env-lazy-map-entry env %))
             (sm-env-keys env))))

       (containsKey [k] (sm-env-contains? env k))
       (entryAt [k] (sm-env-find env k))
       (equiv [other]
         (= (p.ent/entity env)
            (cond-> other
              (smart-map? other)
              (-> sm-env p.ent/entity))))
       (empty [] (sm-env-empty env))
       (count [] (count (p.ent/entity env)))
       (assoc [k v] (sm-env-assoc env k v))
       (without [k] (sm-env-dissoc env k))
       (seq [] (some->> (sm-env-keys env)
                        (map #(sm-env-lazy-map-entry env %))))

       (meta [] (sm-env-meta env))
       (withMeta [meta] (sm-env-with-meta env meta))

       (toString [] (sm-env-to-string env))))

   :cljs
   #_{:clj-kondo/ignore [:private-call]}
   (deftype SmartMap [env]
     Object
     (toString [_] (sm-env-to-string env))
     (equiv [_ other] (-equiv (p.ent/entity env) other))

     ;; ES6
     (keys [_] (es6-iterator (sm-env-keys env)))
     (entries [_] (es6-entries-iterator (seq (p.ent/entity env))))
     (values [_] (es6-iterator (vals (p.ent/entity env))))
     (has [_ k] (sm-env-contains? env k))
     (get [_ k not-found] (-lookup (p.ent/entity env) k not-found))
     (forEach [_ f] (doseq [[k v] (p.ent/entity env)] (f v k)))

     ICloneable
     (-clone [_] (smart-map env (p.ent/entity env)))

     IWithMeta
     (-with-meta [_ new-meta] (sm-env-with-meta env new-meta))

     IMeta
     (-meta [_] (sm-env-meta env))

     ICollection
     (-conj [coll entry]
            (associative-conj coll entry))

     IEmptyableCollection
     (-empty [_] (sm-env-empty env))

     IEquiv
     (-equiv [_ other] (-equiv (p.ent/entity env) other))

     IHash
     (-hash [_] (hash (p.ent/entity env)))

     ISeqable
     (-seq [_]
           (some->> (seq (sm-env-keys env))
                    (map #(->SmartMapEntry env %))))

     ICounted
     (-count [_] (count (p.ent/entity env)))

     ILookup
     (-lookup [_ k] (sm-env-get env k nil))
     (-lookup [_ k not-found] (sm-env-get env k not-found))

     IAssociative
     (-assoc [_ k v] (sm-env-assoc env k v))
     (-contains-key? [_ k] (sm-env-contains? env k))

     IFind
     (-find [_ k] (sm-env-find env k))

     IMap
     (-dissoc [_ k] (sm-env-dissoc env k))

     IKVReduce
     (-kv-reduce [_ f init]
                 (reduce-kv (fn [cur k v] (f cur k (wrap-smart-map env v))) init (p.ent/entity env)))

     IIterable
     (-iterator [_this]
                (transformer-iterator (map #(->SmartMapEntry env %))
                                      (-iterator (vec (sm-env-keys env))) false))

     IReduce
     (-reduce [coll f] (iter-reduce coll f))
     (-reduce [coll f start] (iter-reduce coll f start))

     IFn
     (-invoke [_ k] (sm-env-get env k))
     (-invoke [_ k not-found] (sm-env-get env k not-found))

     IPrintWithWriter
     (-pr-writer [_ writer opts]
                 (-pr-writer (p.ent/entity env) writer opts)))

   :clj
   (def-map-type SmartMap [env]
     (get [_ k default-value] (sm-env-get env k default-value))
     (assoc [_ k v] (sm-env-assoc env k v))
     (dissoc [_ k] (sm-env-dissoc env k))
     (keys [_] (sm-env-keys env))
     (meta [_] (sm-env-meta env))
     (empty [_] (sm-env-empty env))
     (with-meta [_ new-meta] (sm-env-with-meta env new-meta))
     (entryAt [_ k] (sm-env-find env k))
     (toString [_] (sm-env-to-string env))))

(defn smart-map? [x]
  #?(:bb
     (boolean (get smart-map ::env))
     :default
     (instance? SmartMap x)))

(>def ::smart-map smart-map?)

(defn datafy-smart-map
  "Returns a map with all reachable keys from the smart map. The cached keys get their
  value in this map, unresolved keys return with the special value `::pcr/unknown-value`.

  This is helpful to use with REPL tools that support the Clojure Datafiable/Navitable
  protocols. On navigation Pathom try to load the attribute you navigate to."
  [sm]
  (let [env (sm-env sm)
        ent (p.ent/entity env)]
    (-> (into (empty ent)
              (map (fn [attr]
                     (if-let [x (find ent attr)]
                       x
                       (coll/make-map-entry attr :com.wsscode.pathom3.connect.operation/unknown-value))))
              (pci/reachable-attributes env ent))
        (vary-meta assoc
                   `d/nav
                   (fn [_ k v]
                     (if (refs/kw-identical? v :com.wsscode.pathom3.connect.operation/unknown-value)
                       (get sm k)
                       v))))))

#?(:bb
   nil

   :default
   (extend-type SmartMap
     d/Datafiable
     (datafy [sm] (datafy-smart-map sm))))

; endregion

(>defn sm-env
  "Extract the env map from the smart map."
  [;^SmartMap
   smart-map]
  [::smart-map => map?]
  #?(:bb
     (get smart-map ::env)

     :default
     (.-env smart-map)))

(>defn sm-update-env
  "Update smart map environment"
  [;^SmartMap
   sm f & args]
  [::smart-map fn? (s/* any?) => ::smart-map]
  (let [env (apply f (sm-env sm) args)]
    (smart-map env (::source-context env))))

(>defn sm-get-with-stats
  "Return the graph run analysis, use for debugging. You can find the get value return
  in the ::psm/value key.

  Note that if the value is cached, there will be a blank stats."
  ([;^SmartMap
    sm k]
   [::smart-map any? => any?]
   (let [{::p.ent/keys [entity-tree*] :as env} (sm-env sm)
         ast       {:type     :root
                    :children [{:type :prop, :dispatch-key k, :key k}]}
         run-stats (-> (pcr/run-graph! env ast entity-tree*)
                       meta ::pcr/run-stats)]
     (assoc run-stats ::value (wrap-smart-map env (get @entity-tree* k))))))

(>defn sm-replace-context
  "Replace the context data for the smart map with new-context, keeping the
  same environment. This returns a new smart map."
  [;^SmartMap
   sm new-context]
  [::smart-map map? => ::smart-map]
  (smart-map (sm-env sm) new-context))

(defn sm-entity
  "Return the source cached entity for the smart map"
  [sm]
  (-> sm sm-env p.ent/entity))

(>defn sm-assoc!
  "Assoc on the smart map in place, this function mutates the current cache and return
  the same instance of smart map.

  You should use this only in cases where the optimization is required, try starting
  with the immutable versions first, given this has side effects and so more error phone."
  [;^SmartMap
   smart-map k v]
  [::smart-map any? any? => ::smart-map]
  (p.ent/swap-entity! (sm-env smart-map) assoc k v)
  smart-map)

(>defn sm-dissoc!
  "Dissoc on the smart map in place, this function mutates the current cache and return
  the same instance of smart map.

  You should use this only in cases where the optimization is required, try starting
  with the immutable versions first, given this has side effects and so more error phone."
  [;^SmartMap
   smart-map k]
  [::smart-map any? => ::smart-map]
  (p.ent/swap-entity! (sm-env smart-map) dissoc k)
  smart-map)

(>defn sm-touch-ast!
  [;^SmartMap
   smart-map ast]
  [::smart-map :edn-query-language.ast/node
   => ::smart-map]
  (let [env (sm-env smart-map)]
    (pcr/run-graph! env ast (::p.ent/entity-tree* env))
    smart-map))

(>defn sm-touch!
  "Will pre-fetch data in a smart map, given the EQL request. Use this to optimize the
  load of data ahead of time, instead of pulling one by one lazily."
  [;^SmartMap
   smart-map eql]
  [::smart-map ::eql/query
   => ::smart-map]
  (sm-touch-ast! smart-map (eql/query->ast eql)))

(>defn ;^SmartMap
  smart-map
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
  ([{::keys [persistent-cache?]
     :or    {persistent-cache? true}
     :as    env} context]
   [(s/keys :opt [::pci/index-oir])
    map? => ::smart-map]
   (->SmartMap
     (-> env
         (assoc ::smart-map? true)
         (coll/merge-defaults
           (cond-> {:com.wsscode.pathom3.connect.planner/plan-cache* (atom {})}
             persistent-cache?
             (assoc ::pcr/resolver-cache* (atom {}))))
         (p.ent/with-entity context)
         (assoc ::source-context context)))))
