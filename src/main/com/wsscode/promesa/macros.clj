(ns com.wsscode.promesa.macros
  (:require
    [promesa.core :as p]))

(defmacro clet [bindings & body]
  (assert (even? (count bindings)))
  (let [binds (reverse (partition 2 bindings))]
    (reduce
      (fn [acc [l r]]
        `(let [~l ~r]
           (if (p/promise? ~l)
             (p/then ~l (fn [~l] ~acc))
             ~acc)))
      `(do ~@body)
      binds)))
