(ns com.wsscode.pathom3.connect.foreign
  (:require
    [clojure.string :as str]
    [#?(:clj  com.wsscode.async.async-clj
        :cljs com.wsscode.async.async-cljs)
     :refer [let-chan go-promise <? <?maybe <!maybe]]
    [com.wsscode.misc.core :as misc]
    [com.wsscode.pathom3.connect.indexes :as p.c.i]
    [com.wsscode.pathom3.connect.operation :as p.c.o]
    [com.wsscode.pathom3.connect.planner :as p.c.p]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.error :as p.err]
    [com.wsscode.pathom3.format.eql :as p.f.eql]
    [com.wsscode.pathom3.placeholder :as p.ph]
    [com.wsscode.pathom3.specs :as p.spec]
    [edn-query-language.core :as eql]))

(def index-query
  [{::p.c.i/indexes
    [::p.c.i/index-attributes
     ::p.c.i/index-oir
     ::p.c.i/index-io
     ::p.c.i/idents
     ::p.c.i/autocomplete-ignore
     ::p.c.i/index-resolvers
     ::p.c.i/index-mutations]}])

(defn remove-internal-keys [m]
  (into {} (remove (fn [[k _]] (str/starts-with? (or (namespace k) "") "com.wsscode.pathom"))) m))

(defn compute-foreign-input [{::p.c.p/keys [node] :as env}]
  (let [input  (::p.c.p/input node)
        entity (p.ent/entity env)]
    (select-keys entity (keys input))))

(defn compute-foreign-query
  [{::p.c.p/keys [node] :as env}]
  (let [inputs     (compute-foreign-input env)
        base-query (eql/ast->query (::p.c.p/foreign-ast node))]
    (if (seq inputs)
      (let [ident-join-key (if (= 1 (count inputs))
                             (first (keys inputs))
                             (-> (p.ph/find-closest-non-placeholder-parent-join-key env)
                                 p.f.eql/ident-key))
            join-node      (if (contains? inputs ident-join-key)
                             [ident-join-key (get inputs ident-join-key)]
                             [::foreign-call nil])]
        {::base-query base-query
         ::query      [{(list join-node {:pathom/context (dissoc inputs ident-join-key)}) base-query}]
         ::join-node  join-node})
      {::base-query base-query
       ::query      base-query})))

(defn internalize-foreign-errors
  [{::p.spec/keys [path]
    ::keys        [join-node]} errors]
  (misc/map-keys #(into (pop path) (cond-> % join-node next)) errors))

(defn call-foreign-parser [env parser]
  (let [{::keys [query join-node] :as foreign-call} (compute-foreign-query env)]
    (let-chan [response (parser {} query)]
      (if-let [errors (::p.err/errors response)]
        (->> (internalize-foreign-errors (merge env foreign-call) errors)
             (swap! (::p.err/errors* env) merge)))
      (cond-> response join-node (get join-node)))))

(defn internalize-parser-index*
  ([indexes] (internalize-parser-index* indexes nil))
  ([{::p.c.i/keys [index-source-id] :as indexes} parser]
   (let [index-source-id (or index-source-id (gensym "dynamic-parser-"))]
     (-> indexes
         (update ::p.c.i/index-resolvers
           (fn [resolvers]
             (into {}
                   (map (fn [[r v]] [r (assoc v ::p.c.o/dynamic-name index-source-id)]))
                   resolvers)))
         (assoc-in [::p.c.i/index-resolvers index-source-id]
           (p.c.o/resolver index-source-id
                           {::p.c.o/cache?            false
                            ::p.c.o/dynamic-resolver? true}
                           (fn [env _] (call-foreign-parser env parser))))
         (dissoc ::p.c.i/index-source-id)))))

