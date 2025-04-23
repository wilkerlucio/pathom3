(ns com.wsscode.pathom3.trace
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [=> >def >defn]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.misc.refs :as refs]
    [com.wsscode.misc.time :as time]
    [com.wsscode.pathom3.path :as p.path]
    #?(:clj [com.wsscode.promesa.macros :refer [clet ctry]])))

(>def ::span-id symbol?)
(>def ::span-type "Type of a span" qualified-keyword?)
(>def ::log-type "Type of a log event" qualified-keyword?)
(>def ::parent-span-id ::span-id)
(>def ::root-span-id ::span-id)
(>def ::error string?)
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
(>def ::internal-span? "Tell the visualizer to render the span internally inside the parent span." boolean?)

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

(defn open-span-env!
  "Open a new span, returns env with updated parent-span-id and new span id"
  [env span]
  (let [sid (open-span! env span)]
    (assoc env ::parent-span-id sid)))

(defn close-span!
  "Closes a span and adds it to the trace."
  ([env] (close-span! env (::parent-span-id env)))
  ([env span-id]
   (add-signal! env {::signal-type ::signal-close-span
                     ::span-id     span-id
                     ::end-time    (time/now-ms)})
   span-id))

(defn under-span
  "Returns a new environment setting the context span id."
  [env span-id]
  (assoc env ::parent-span-id span-id))

(defn set-attributes!
  "Create a new entry to add/update fields from a span. It will use
  the ::parent-span-id from env to find the span, unless the user specifies it."
  ([env attributes]
   (set-attributes! env (::parent-span-id env) attributes))
  ([env span-id attributes]
   (add-signal! env {::signal-type ::signal-attributes
                     ::span-id     span-id
                     ::attributes  attributes})))

(defn mark-error!
  "Helper to set error attribute."
  [env e]
  (set-attributes! env {::error (ex-message e)}))

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
        (if (::trace* env#)
          (let [span#    (dissoc ~span ::env)
                span-id# (open-span! env# span#)
                res#     (let [~sym (under-span env# span-id#)]
                           (try
                             ~@body
                             (catch #?(:clj Throwable :cljs :default) error#
                               (mark-error! ~sym error#)
                               (throw error#))
                             (finally
                               (close-span! ~sym span-id#))))]
            res#)
          (let [~sym env#] ~@body))
        (throw (ex-info "With span requires environment as part of the data" {})))))

#?(:clj
   (defmacro with-span-async!
     "Like with-span! but supports async body. This can also be used with sync processes, with the adding overhead of
     checking for a promise. The reason to support sync is that so it can be used in generic functions that support both
     sync and async."
     [[sym span] & body]
     `(if-let [env# (get ~span ::env)]
        (if (::trace* env#)
          (clet [span#    (dissoc ~span ::env)
                 span-id# (open-span! env# span#)
                 res#     (let [~sym (under-span env# span-id#)]
                            (ctry
                              ~@body
                              (catch #?(:clj Throwable :cljs :default) error#
                                (mark-error! ~sym error#)
                                (close-span! ~sym span-id#)
                                (throw error#))))]
            (close-span! env# span-id#)
            res#)
          (let [~sym env#] ~@body))
        (throw (ex-info "With span requires environment as part of the data" {})))))

(defn start-tracing!
  "Helper to set up the tracing requirements. This will include the trace atom in env (unless its already there) and
  create the root span for the tracing (also set it as the parent-span-id).

  This function is idempotent, if it sees a root-span-id already in the environment, it will just return env
  as-is."
  [env]
  (if (::root-span-id env)
    env
    (let [env'         (assoc env ::trace* (or (::trace* env) (atom [])))
          root-span-id (open-span! env' {::span-type  ::trace-root
                                         ::attributes {::label " "}})
          env'         (assoc env'
                         ::parent-span-id root-span-id
                         ::root-span-id root-span-id)]
      env')))

(defn end-tracing!
  "Closes up the tracing, it will finish the root span and return the trace data."
  [{::keys [root-span-id trace*] :as env}]
  (close-span! env root-span-id)
  (some-> trace* deref))

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
             (sort-by ::start-time))))))

(defn trace->tree
  "Convert the trace events into a trace tree."
  [trace]
  (trace->tree* (normalize-trace trace) nil))

(defn live-trace!
  "Helper to react to trace changes and immediately print them
  to the output as they come."
  [trace-atom]
  (add-watch trace-atom :live
    (fn [_ _ _ n]
      (let [evt (peek n)]
        (print (str (pr-str [(::span-type evt) (dissoc evt ::span-type)]) "\n"))))))
