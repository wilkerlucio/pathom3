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
