(ns com.wsscode.pathom3.trace
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [=> >def >defn]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.misc.refs :as refs]
    [com.wsscode.misc.time :as time]
    [com.wsscode.pathom3.path :as p.path]
    [com.wsscode.promesa.macros :refer [clet]]))

(>def ::span-id symbol?)
(>def ::span-type "Type of a span" qualified-keyword?)
(>def ::log-type "Type of a log event" qualified-keyword?)
(>def ::parent-span-id ::span-id)
(>def ::timestamp nat-int?)
(>def ::start-time ::timestamp)
(>def ::end-time ::timestamp)
(>def ::attributes (s/keys))

(>def ::span (s/keys
               :req [::span-id ::span-type ::start-time]
               :opt [::end-time ::attributes ::parent-span-id]))

(>def ::log-event
  (s/keys :req [::timestamp] :opt [::attributes]))

(>def ::trace (s/coll-of ::signal :kind vector?))
(>def ::trace* "Atom with ::details." refs/atom?)

; region signal

(>def ::signal-type #{::signal-open-span ::signal-close-span ::signal-log-event ::signal-attributes})

(defmulti signal-type ::signal-type)

(defmethod signal-type ::signal-open-span [_]
  (s/keys :req [::signal-type ::span-id ::span-type ::start-time]
          :opt [::parent-span-id]))

(defmethod signal-type ::signal-close-span [_]
  (s/keys :req [::signal-type ::span-id ::end-time]))

(defmethod signal-type ::signal-log-event [_]
  (s/keys :req [::signal-type ::span-id ::log-type ::timestamp]))

(defmethod signal-type ::signal-attributes [_]
  (s/keys :req [::signal-type ::span-id ::attributes]))

(>def ::signal (s/multi-spec signal-type ::signal-type))

; endregion

; region built-in attribute ontology

(>def ::label "A string (usually short) describing the span." string?)
(>def ::style "Map with CSS styles to apply in the trace bar." map?)

; endregion

(defn new-span-id [] (gensym "pathom3-span-"))

(>defn add-signal!
  "Adds a signal to the trace. This is a low-level function, you should use the other functions to add signals to the trace."
  [{::keys [trace*]} signal]
  [map? ::signal => ::span-id]
  (when trace*
    (swap! trace* conj signal)
    (::span-id signal)))

(defn open-span!
  "Opens a new span and adds it to the trace. Returns the span id."
  [{::keys        [parent-span-id]
    ::p.path/keys [path]
    :as           env} span]
  (add-signal! env
               (-> span
                   (assoc ::signal-type ::signal-open-span, ::start-time (time/now-ms))
                   (assoc-in [::attributes ::p.path/path] (or path []))
                   (cond->
                     (not (::span-id span)) (assoc ::span-id (new-span-id))
                     (and parent-span-id (not (::parent-span-id span))) (assoc ::parent-span-id parent-span-id)))))

(defn close-span!
  "Closes a span and adds it to the trace."
  [env span-id]
  (add-signal! env {::signal-type ::signal-close-span
                    ::span-id     span-id
                    ::end-time    (time/now-ms)})
  span-id)

(defn under-span
  "Returns a new environment setting the context span id."
  [env span-id]
  (assoc env ::parent-span-id span-id))

(defn set-attributes!
  "Create a new entry to add/update fields from a span. It will use
  the ::parent-span-id from env to find the span, unless the user specifies it."
  ([env fields]
   (set-attributes! env (::parent-span-id env) fields))
  ([env span-id fields]
   (assert span-id "Can't set fields without an span-id")
   (add-signal! env {::signal-type ::signal-attributes
                     ::span-id     span-id
                     ::attributes  fields})))

(defn log-event!
  ([env log] (log-event! env (::parent-span-id env) log))
  ([env span-id log]
   (add-signal! env
                (assoc log
                  ::signal-type ::signal-log-event
                  ::span-id span-id
                  ::timestamp (time/now-ms)))))

#?(:clj
   (defmacro with-span!
     "Opens a new span and closes it after the body is executed. The span id is bound to the environment.

        (t/with-span! [env {::t/env env}]
          (do-something))"
     [[sym span] & body]
     `(if-let [env# (get ~span ::env)]
        (let [span#    (dissoc ~span ::env)
              span-id# (open-span! env# span#)
              res#     (let [~sym (under-span env# span-id#)]
                         ~@body)]
          (close-span! env# span-id#)
          res#)
        (throw (ex-info "With span requires environment as part of the data" {})))))

(>defn normalize-trace
  "Normalize the trace, this will accumulate the fields and find the duration of an event.
  The result is a map indexed by span-id. For each item there will also be a ::span-children
  with the ids of the children spans. This will later help to expand the normalized spans into
  a tree."
  [trace]
  [::trace => any?]
  (reduce
    (fn [tree {::keys [span-id parent-span-id signal-type] :as signal}]
      (case signal-type
        ::signal-open-span
        (-> (assoc tree span-id (dissoc signal ::signal-type))
            (update-in [parent-span-id ::span-children] coll/sconj span-id))

        ::signal-close-span
        (assoc-in tree [span-id ::end-time] (::end-time signal))

        ::signal-attributes
        (update-in tree [span-id ::attributes] merge (::attributes signal))

        ::signal-log-event
        (update-in tree [span-id ::events] coll/vconj signal)))
    {}
    trace))

(defn trace->tree* [normalized span-id]
  (let [span (get normalized span-id)]
    (update span ::span-children
      (fn [children]
        (->> (mapv #(trace->tree* normalized %) children)
             (sort-by ::timestamp))))))

(defn trace->tree
  "Convert the trace events into a trace tree."
  [trace]
  (trace->tree* (normalize-trace trace) nil))

(defn wrap-parser-trace [wrap-root-run-graph]
  (fn wrap-parser-trace-internal [env ast-or-graph entity]
    (let [ast (or (:edn-query-language.ast/node ast-or-graph)
                  ast-or-graph)]
      (if (some #(-> % :key (= ::trace)) (:children ast))
        (let [trace* (or (::trace* env) (atom []))
              env'   (assoc env ::trace* trace*)]
          (clet [res (wrap-root-run-graph env' ast-or-graph entity)]
            (add-signal! env' {::span-type ::trace-done})
            #_(assoc res ::trace (trace->viz @trace*))
            res))
        (wrap-root-run-graph env ast-or-graph entity)))))

(def trace-plugin
  {:com.wsscode.pathom3.plugin/id
   `trace-plugin

   :com.wsscode.pathom3.connect.runner/wrap-root-run-graph!
   wrap-parser-trace

   :com.wsscode.pathom.connect/register
   [{:com.wsscode.pathom.connect/sym     `add-signal!
     :com.wsscode.pathom.connect/output  [:com.wsscode.pathom/trace]
     :com.wsscode.pathom.connect/resolve (fn [_env _] {:com.wsscode.pathom/trace nil})}]})

(defn live-trace!
  "Helper to react to trace changes and immediately print them
  to the output as they come."
  [trace-atom]
  (add-watch trace-atom :live
    (fn [_ _ _ n]
      (let [evt (peek n)]
        (print (str (pr-str [(::span-type evt) (dissoc evt ::span-type)]) "\n"))))))
