(ns com.wsscode.pathom3.trace
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [=> >def >defn]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.misc.time :as time]
    [com.wsscode.promesa.macros :refer [clet]]))

(>def ::span-id symbol?)
(>def ::parent-id ::span-id)
(>def ::event-type keyword?)
(>def ::direction #{::direction-enter ::direction-leave})
(>def ::timestamp nat-int?)
(>def ::duration nat-int?)
(>def ::fields map?)
(>def ::event (s/keys :req [::span-id ::event-type] :opt [::timestamp ::fields ::direction]))

(>def ::trace (s/coll-of ::event :kind vector?))
(>def ::trace* "Atom with ::details." any?)

;; special known fields
(>def ::label string?)
(>def ::style "Map with CSS styles to apply in the trace bar." map?)

(defmacro span-sym [] `(gensym "pathom3-span-"))

(defn trace [{::keys [trace* parent-id]} event]
  (when trace*
    (let [event' (-> event
                     (assoc ::timestamp (time/now-ms))
                     (cond->
                       (not (::span-id event)) (assoc ::span-id (span-sym))
                       (and parent-id (not (::parent-id event))) (assoc ::parent-id parent-id)))]
      (swap! trace* conj event')
      (::span-id event'))))

(defn start-span [env event]
  (trace env (assoc event ::direction ::direction-enter)))

(defn finish-span [env span-id]
  (trace env {::direction ::direction-leave ::span-id span-id})
  span-id)

(defn span-fields
  "Create a new entry to add/update fields from a span. It will use
  the ::parent-id from env to find the span, unless the user specifies it."
  ([env fields]
   (span-fields env (::parent-id env) fields))
  ([env span-id fields]
   (assert span-id "Can't set fields without an span-id")
   (trace (dissoc env ::parent-id) {::span-id span-id ::fields fields})))

#?(:clj
   (defmacro tracing
     "Track the body with a new span. Use this version when you don't expect children
     spans. If you have children spans, use `tracing-with-parent`."
     [env event & body]
     `(if (get ~env ::trace*)
        (let [span-id# (start-span ~env ~event)
              res#     (do ~@body)]
          (finish-span ~env span-id#)
          res#)
        (do ~@body))))

(defn tracing-with-parent
  "Trace the body setting the parent-id. This will help, so the env inside the f
  will have parent-id set to the newly created span. So any new spans will automatically
  have the parent-id assigned."
  [env event f]
  (if (get env ::trace*)
    (let [span-id (start-span env event)
          res     (f (assoc env ::parent-id span-id))]
      (finish-span env span-id)
      res)
    (f env)))

(>defn normalize-trace
  "Normalize the trace, this will accumulate the fields and find the duration of an event.
  The result is a map indexed by span-id. For each item there will also be a ::span-children
  with the ids of the children spans. This will later help to expand the normalized spans into
  a tree."
  [trace]
  [::trace => any?]
  (reduce
    (fn [tree {::keys [span-id parent-id direction] :as event}]
      (cond
        (= ::direction-enter direction)
        (-> (assoc tree span-id event)
            (update-in [parent-id ::span-children] coll/sconj span-id))

        (= ::direction-leave direction)
        (assoc-in tree [span-id ::duration] (- (::timestamp event) (get-in tree [span-id ::timestamp])))

        :else
        (update-in tree [span-id ::fields] merge (::fields event))))
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
  (let [normalized (normalize-trace trace)]
    (trace->tree* normalized nil)))

(defn live-trace!
  "Helper to react to trace changes and immediately print them
  to the output as they come."
  [trace-atom]
  (add-watch trace-atom :live
    (fn [_ _ _ n]
      (let [evt (peek n)]
        (print (str (pr-str [(::event-type evt) (dissoc evt ::event-type)]) "\n"))))))

(defn wrap-parser-trace [wrap-root-run-graph]
  (fn wrap-parser-trace-internal [env ast-or-graph entity]
    (let [ast (or (:edn-query-language.ast/node ast-or-graph)
                  ast-or-graph)]
      (if (some #(-> % :key (= ::trace)) (:children ast))
        (let [trace* (or (::trace* env) (atom []))
              env'   (assoc env ::trace* trace*)]
          (clet [res (wrap-root-run-graph env' ast-or-graph entity)]
            (trace env' {::event-type ::trace-done})
            #_(assoc res ::trace (trace->viz @trace*))
            res))
        (wrap-root-run-graph env ast-or-graph entity)))))

(def trace-plugin
  {:com.wsscode.pathom3.plugin/id
   `trace-plugin

   :com.wsscode.pathom3.connect.runner/wrap-root-run-graph!
   wrap-parser-trace

   :com.wsscode.pathom.connect/register
   [{:com.wsscode.pathom.connect/sym     `trace
     :com.wsscode.pathom.connect/output  [:com.wsscode.pathom/trace]
     :com.wsscode.pathom.connect/resolve (fn [_env _] {:com.wsscode.pathom/trace nil})}]})
