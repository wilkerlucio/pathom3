(ns com.wsscode.pathom3.trace
  (:require
    [clojure.set :as set]
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [>def]]
    [com.wsscode.misc.coll :as coll]
    [com.wsscode.misc.refs :as refs]
    [com.wsscode.misc.time :as time])
  #?(:clj
     (:import
       (java.security
         SecureRandom))))

; region specs

(>def ::type #{::type-close-span
               ::type-log
               ::type-set-attributes})

(>def ::attributes (s/keys))
(>def ::span-id string?)

(>def ::timestamp pos-int?)
(>def ::start-time ::timestamp)
(>def ::end-time ::timestamp)

(>def ::events (s/coll-of (s/keys)))

(>def ::name string?)
(>def ::error string?)

(>def ::parent-span-id ::span-id)

(>def ::trace* "Atom holding trace data" refs/atom?)

; endregion

; region id generation

#?(:clj
   (defn generate-bytes
     "Generates a 64-bit Span ID for OpenTelemetry."
     [size]
     (let [random-bytes (byte-array size)]
       (.nextBytes (SecureRandom.) random-bytes)
       random-bytes)))

#?(:clj
   (defn bytes-to-hex
     "Converts a byte array to a hex string."
     [byte-array]
     (apply str "0x" (mapv #(format "%02x" (bit-and 0xFF %)) byte-array))))

(defn new-trace-id []
  #?(:clj  (bytes-to-hex (generate-bytes 16))
     :cljs (gensym "trace-id")))

(defn new-span-id []
  #?(:clj  (bytes-to-hex (generate-bytes 8))
     :cljs (gensym "span-id")))

; endregion

(defn add-signal!
  "Adds a signal to the trace. This is a low-level function, you should use the other functions to add signals to the trace."
  [{::keys [trace* parent-span-id]} entry]
  (when trace*
    (swap! trace* conj (assoc entry ::timestamp (time/now-ms)
                         ::parent-span-id parent-span-id))))

(defn open-span!
  "Opens a new span and adds it to the trace. Returns the span id."
  [env span]
  (let [id (new-span-id)]
    (add-signal! env (-> span
                         (assoc ::span-id id)
                         (update ::attributes assoc :com.wsscode.pathom3.path/path (get env :com.wsscode.pathom3.path/path []))))
    id))

(defn close-span!
  "Closes a span and adds it to the trace."
  [env span-id]
  (add-signal! env {::type ::type-close-span ::close-span-id span-id}))

(defn under-span
  "Returns a new environment setting the context span id."
  [env span-id]
  (assoc env ::parent-span-id span-id))

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

#?(:clj
   (s/fdef with-span!
     :args (s/cat :binding (s/tuple simple-symbol? any?) :body any?)
     :ret any?))

(defn log!
  "Adds a log entry to the trace. This will end up as an event to the current span."
  [env log-entry]
  (add-signal! env (assoc log-entry ::type ::type-log)))

(defn set-attributes! [env & {:as attributes}]
  (add-signal! env {::type       ::type-set-attributes
                    ::attributes attributes}))

(defn fold-trace
  "Folds the trace data into spans. This materializes the trace data, making it suitable for usage."
  [trace]
  (let [{:keys [top-level props]}
        (-> (reduce
              (fn [out {::keys [type] :as entry}]
                (case type
                  (::type-log ::type-set-attributes)
                  (update-in out [:props (::parent-span-id entry)] coll/vconj entry)

                  ::type-close-span
                  (update-in out [:props (::close-span-id entry)] coll/vconj entry)

                  ; else
                  (update out :top-level conj! (set/rename-keys entry {::timestamp ::start-time}))))
              {:top-level (transient [])
               :props     {}}
              trace)
            (update :top-level persistent!))]
    (->> top-level
         (map
           (fn [entry]
             (if-let [props (get props (::span-id entry))]
               (reduce
                 (fn [entry {::keys [type] :as ext}]
                   (case type
                     ::type-log
                     (update entry ::events coll/vconj (dissoc ext ::parent-span-id))

                     ::type-set-attributes
                     (update entry ::attributes merge (::attributes ext))

                     ::type-close-span
                     (assoc entry ::end-time (::timestamp ext))))
                 entry
                 props)
               entry))))))
