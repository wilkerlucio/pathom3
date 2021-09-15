(ns com.wsscode.pathom3.test.helpers
  (:require
    [clojure.walk :as walk]))

(defn spy [{:keys [return]}]
  (let [calls (atom [])]
    (with-meta
      (fn [& args]
        (swap! calls conj args)
        return)
      {:calls calls})))

(defn spy-fn [f]
  (let [calls (atom [])]
    (with-meta
      (fn [& args]
        (swap! calls conj args)
        (apply f args))
      {:calls calls})))

(defn match-error [error-msg-regex]
  (fn [value]
    (re-find error-msg-regex (ex-message value))))

(defn expose-meta [x]
  (walk/postwalk
    (fn [x]
      (if (and (map? x)
               (seq (meta x)))
        (assoc x ::meta (meta x))
        x))
    x))
