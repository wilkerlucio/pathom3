(ns com.wsscode.misc.refs
  #?(:clj
     (:import
       (clojure.lang
         IDeref))))

(defn atom? [x]
  #?(:clj  (instance? IDeref x)
     :cljs (satisfies? IDeref x)))
