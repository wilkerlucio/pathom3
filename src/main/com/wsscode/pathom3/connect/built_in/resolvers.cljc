(ns com.wsscode.pathom3.connect.built-in.resolvers
  (:require
    [com.wsscode.pathom3.connect.operation :as pco]))

(defn attr-alias-name [from to]
  (symbol (str (munge (subs (str from) 1)) "->" (munge (subs (str to) 1)))))

(defn alias-resolver
  "Create a resolver that will convert property `from` to a property `to` with
  the same value. This only creates the alias in one direction"
  [from to]
  (pco/resolver (attr-alias-name from to)
                {::pco/input  [from]
                 ::pco/output [to]}
                (fn [_ input] {to (get input from)})))

(defn alias-resolver2
  "Like alias-resolver, but returns a vector containing the alias in both directions."
  [from to]
  [(alias-resolver from to)
   (alias-resolver to from)])
