(ns com.wsscode.misc.math)

(defn round [x]
  #?(:clj  (Math/round ^double x)
     :cljs (Math/round x)))
