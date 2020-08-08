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
    [potemkin.collections :refer [def-map-type]]))

(declare smart-map ->SmartMap)

(defn wrap-smart-map [env x]
  (cond
    (map? x)
    (smart-map env x)

    (sequential? x)
    (map #(wrap-smart-map env %) x)

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
  (keys @(get env ::p.ent/cache-tree*)))

(defn sm-meta
  "Returns meta data of smart map. This will add the special ::env key on the meta, this
  is the value of the environment on this smart map."
  [env]
  (-> (meta @(get env ::p.ent/cache-tree*))
      (assoc ::env env)))

(defn sm-with-meta [env meta]
  (smart-map env (with-meta @(get env ::p.ent/cache-tree*) meta)))

(def-map-type SmartMap [env]
  (get [_ k default-value]
       (sm-get env k default-value))
  (assoc [_ k v]
    (sm-assoc env k v))
  (dissoc [_ k]
          (sm-dissoc env k))
  (keys [_]
        (sm-keys env))
  (meta [_]
        (sm-meta env))
  (with-meta [_ mta]
    (sm-with-meta env mta)))

(>def ::smart-map #(instance? SmartMap %))

(defn sm-assoc!
  "Assoc on the smart map in place, this function mutates the current cache and return
  the same instance of smart map.

  You should use this only in cases where the optimization is required, try starting
  with the immutable versions first, given this has side effects and so more error phone."
  [smart-map k v]
  (swap! (-> smart-map meta ::env ::p.ent/cache-tree*) assoc k v)
  smart-map)

(defn sm-dissoc! [smart-map k]
  (swap! (-> smart-map meta ::env ::p.ent/cache-tree*) dissoc k)
  smart-map)

(>defn smart-map
  [env context]
  [(s/keys :req [:com.wsscode.pathom3.connect.indexes/index-oir])
   map? => ::smart-map]
  (->SmartMap
    (-> env
        (p.ent/with-cache-tree context)
        (assoc ::source-context context))))
