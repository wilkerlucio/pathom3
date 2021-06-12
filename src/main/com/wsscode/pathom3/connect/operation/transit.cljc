(ns com.wsscode.pathom3.connect.operation.transit
  (:require
    [cognitect.transit :as t]
    [com.wsscode.pathom3.connect.operation
     #?@(:cljs [:refer [Resolver Mutation]])
     :as pco])
  #?(:clj
     (:import
       (com.wsscode.pathom3.connect.operation
         Mutation
         Resolver))))

(defn restored-handler [_ _]
  (throw (ex-info "This operation came serialized via network, it doesn't have an implementation." {})))

(def read-handlers
  {"pathom3/Resolver" (t/read-handler #(-> % (assoc ::pco/resolve restored-handler) pco/resolver))
   "pathom3/Mutation" (t/read-handler #(-> % (assoc ::pco/mutate restored-handler) pco/mutation))})

(def write-handlers
  {Resolver (t/write-handler (fn [_] "pathom3/Resolver") pco/operation-config)
   Mutation (t/write-handler (fn [_] "pathom3/Mutation") pco/operation-config)})
