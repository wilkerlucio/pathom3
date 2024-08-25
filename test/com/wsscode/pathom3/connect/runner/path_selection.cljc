(ns com.wsscode.pathom3.connect.runner.path-selection
  (:require
    [clojure.test :refer [deftest]]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.connect.runner.helpers :as rr :refer [check-all-runners]]))

(deftest run-graph-input-size-sort
  (let [env (pci/register
              [(pco/resolver 'resolve-full-name-by-first-name
                 {::pco/input  [:person/first-name]
                  ::pco/output [:person/full-name]}
                 (fn [_ {:person/keys [first-name]}]
                   {:person/full-name first-name}))

               (pco/resolver 'resolve-full-name-with-first-and-last-name
                 {::pco/input  [:person/first-name :person/last-name]
                  ::pco/output [:person/full-name]}
                 (fn [_ {:person/keys [first-name last-name]}]
                   {:person/full-name (str first-name " " last-name)}))])]
    (check-all-runners
      env
      {:person/first-name "Björn", :person/last-name "Ebbinghaus"}
      [:person/full-name]
      {:person/full-name "Björn Ebbinghaus"})

    (check-all-runners
      env
      {:person/first-name "Björn"}
      [:person/full-name]
      {:person/full-name "Björn"})))
