(ns com.wsscode.pathom3.plugin-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.plugin :as p.plugin]))

(def op-plugin
  {::p.plugin/id
   'foo

   ::wrap-operation
   (fn [op] (fn [x] (op (+ x 10))))})

(def plugin-env
  (p.plugin/add-plugin
    op-plugin))

(deftest add-plugin-test
  (is (= plugin-env
         {:com.wsscode.pathom3.plugin/index-plugins
          {'foo op-plugin},
          :com.wsscode.pathom3.plugin/plugin-order
          ['foo],
          :com.wsscode.pathom3.plugin/plugin-actions
          {:com.wsscode.pathom3.plugin-test/wrap-operation
           [(::wrap-operation op-plugin)]}})))

(deftest run-with-plugins-test
  (is (= (p.plugin/run-with-plugins plugin-env
                                    ::unavailable (fn [x] (* 2 x)) 5)
         10))
  (is (= (p.plugin/run-with-plugins plugin-env
                                    ::wrap-operation (fn [x] (* 2 x)) 5)
         30)))
