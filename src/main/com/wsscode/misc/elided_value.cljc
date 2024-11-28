(ns com.wsscode.misc.elided-value)

(def ELIDE_TEXT "Elided value, deref to read it.")

(deftype ElidedValue [x]
  #?@(:clj [clojure.lang.IDeref
            (deref [_] x)]
      :cljs [IDeref
             (-deref [_] x)])

  #?@(:cljs [IPrintWithWriter
             (-pr-writer [_ writer _]
                         (write-all writer ELIDE_TEXT))])

  Object
  (toString [_] ELIDE_TEXT))

#?(:clj
   (do (defmethod print-method ElidedValue [_ ^java.io.Writer writer]
         (print-method ELIDE_TEXT writer))

       (defmethod print-dup ElidedValue [_ ^java.io.Writer writer]
         (print-method ELIDE_TEXT writer))))

(defn elided-value
  "Wraps a value with ElidedValue. This will affect how it prints, it will always print the same text. To get the original
  value, you must deref it."
  [x]
  (->ElidedValue x))
