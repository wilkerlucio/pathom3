(ns com.wsscode.pathom3.connect.runner.helpers
  (:require
    [check.core :refer [#_ :clj-kondo/ignore => check]]
    [clojure.spec.alpha :as s]
    [clojure.test :refer [testing]]
    [com.wsscode.pathom3.connect.runner :as pcr]
    [com.wsscode.pathom3.connect.runner.async :as pcra]
    [com.wsscode.pathom3.connect.runner.parallel :as pcrc]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.format.eql :as pf.eql]
    [edn-query-language.core :as eql]
    [matcher-combinators.test]
    [promesa.core :as p])
  #?(:cljs
     (:require-macros
       [com.wsscode.pathom3.connect.runner.helpers])))

(defn match-keys? [ks]
  (fn [m]
    (reduce
      (fn [_ k]
        (if-let [v (find m k)]
          (if (s/valid? k (val v))
            true
            (reduced false))
          (reduced false)))
      true
      ks)))

(defn run-graph
  ([{::keys [map-select?] :as env} tree query]
   (let [ast (eql/query->ast query)
         res (pcr/run-graph! env ast (p.ent/create-entity tree))]
     (if map-select?
       (pf.eql/map-select env res query)
       res)))
  ([env tree query _] (run-graph env tree query)))

(defn run-graph-async
  ([env tree query]
   (let [ast (eql/query->ast query)]
     (pcra/run-graph! env ast (p.ent/create-entity tree))))
  ([env tree query _expected]
   (run-graph-async env tree query)))

(defn run-graph-parallel [env tree query]
  (let [ast (eql/query->ast query)]
    (p/timeout
      (pcrc/run-graph! env ast (p.ent/create-entity tree))
      3000)))

(def all-runners [run-graph #?@(:clj [run-graph-async run-graph-parallel])])

#?(:clj
   (defmacro check-serial [env entity tx expected]
     `(check
        (run-graph ~env ~entity ~tx)
        ~'=> ~expected))

   :cljs
   (defn check-serial [env entity tx expected]
     (check
       (run-graph env entity tx)
       => expected)))

#?(:clj
   (defmacro check-parallel [env entity tx expected]
     `(check
        @(run-graph-parallel ~env ~entity ~tx)
        ~'=> ~expected))

   :cljs
   (defn check-parallel [env entity tx expected]
     (check
       @(run-graph-parallel env entity tx)
       => expected)))

#?(:clj
   (defmacro check-all-runners [env entity tx expected]
     `(doseq [runner# all-runners]
        (testing (str runner#)
          (check
            (let [res# (runner# ~env ~entity ~tx)]
              (if (p/promise? res#)
                @res# res#))
            ~'=> ~expected))))

   :cljs
   (defn check-all-runners [env entity tx expected]
     (doseq [runner all-runners]
       (testing (str runner)
         (check
           (let [res (runner env entity tx)]
             (if (p/promise? res)
               @res res))
           => expected)))))
