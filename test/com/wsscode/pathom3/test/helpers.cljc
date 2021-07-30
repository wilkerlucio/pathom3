(ns com.wsscode.pathom3.test.helpers)

(defn spy [{:keys [return]}]
  (let [calls (atom [])]
    (with-meta
      (fn [& args]
        (swap! calls conj args)
        return)
      {:calls calls})))

(defn match-error [error-msg-regex]
  (fn [value]
    (tap> ["V" value error-msg-regex])
    (re-find error-msg-regex (ex-message value))))
