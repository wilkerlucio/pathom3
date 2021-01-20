(ns com.wsscode.promesa.macros
  (:require
    #?(:clj [promesa.core :as p]))
  #?(:cljs
     (:require-macros
       [com.wsscode.promesa.macros])))

#?(:clj
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
           `(let [~l ~r]
              (if (p/promise? ~l)
                (p/then ~l (fn [~l] ~acc))
                ~acc)))
         `(do ~@body)
         binds))))
