(ns com.wsscode.promesa.macros
  #?@(:bb
      []
      :default
      [(:require [promesa.core :as p])])
  #?(:cljs
     (:require-macros
       [com.wsscode.promesa.macros])))

#?(:bb
   (defmacro clet
     "On Babashka this macro just does the same as let."
     [bindings & body]
     (assert (even? (count bindings)))
     `(let ~bindings
        ~@body))

   :clj
   (defmacro clet
     "This is similar to promesa let. But this only returns a promise if some of
     the bindings is a promise. Otherwise returns values as-is. This function is
     intended to use in places that you want to be compatible with both sync
     and async processes."
     [bindings & body]
     (assert (even? (count bindings)))
     (let [binds (reverse (partition 2 bindings))]
       (reduce
         (fn [acc [l r]]
           `(let [r# ~r]
              (if (p/promise? r#)
                (p/then r# (fn [~l] ~acc))
                (let [~l r#]
                  ~acc))))
         `(do ~@body)
         binds))))

#?(:bb
   (defmacro ctry
     "On Babashka this macro does the same as try."
     [& body]
     `(try ~@body))

   :clj
   (defmacro ctry
     "This is a helper to enable catching of both sync and async exceptions.

     (ctry
       (clet [foo (maybe-async-op)]
         (handle-result foo))
       (catch Throwable e
         :error))"
     [& body]
     (let [[_ ex-kind ex-sym & ex-body] (last body)
           body (butlast body)]
       `(try
          (let [res# (do ~@body)]
            (if (p/promise? res#)
              (p/catch res# (fn [~ex-sym] ~@ex-body))
              res#))
          (catch ~ex-kind ~ex-sym ~@ex-body)))))

#?(:bb
   (defmacro p-> [& forms]
     `(-> ~@forms))

   :clj
   (defmacro p-> [x & forms]
     (let [fns (mapv (fn [arg]
                       (let [[f & args] (if (sequential? arg)
                                          arg
                                          (list arg))]
                         `(fn [p#] (~f p# ~@args)))) forms)]
       `(p/chain (p/promise ~x) ~@fns))))

#?(:bb
   (defmacro p->> [& forms]
     `(->> ~@forms))

   :clj
   (defmacro p->> [x & forms]
     (let [fns (mapv (fn [arg]
                       (let [[f & args] (if (sequential? arg)
                                          arg
                                          (list arg))]
                         `(fn [p#] (~f ~@args p#)))) forms)]
       `(p/chain (p/promise ~x) ~@fns))))
