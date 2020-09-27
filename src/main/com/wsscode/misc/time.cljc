(ns com.wsscode.misc.time)

(defn now-ms []
  #?(:clj  (/ (double (System/nanoTime)) 1000000.0)
     :cljs (system-time)))
