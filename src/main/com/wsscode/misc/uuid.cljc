(ns com.wsscode.misc.uuid
  #?(:clj
     (:import
       (java.util
         UUID))))

(defn cljc-random-uuid []
  #?(:clj  (UUID/randomUUID)
     :cljs (random-uuid)))
