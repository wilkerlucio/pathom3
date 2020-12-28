(ns com.wsscode.pathom3.plugin-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.plugin :as p.plugin]))

(p.plugin/defplugin op-plugin
  {::wrap-operation
   (fn [op] (fn [x] (conj (op (conj x :e1)) :x1)))})

(p.plugin/defplugin op-plugin2
  {::wrap-operation
   (fn [op] (fn [x] (conj (op (conj x :e2)) :x2)))})

(p.plugin/defplugin op-plugin3
  {::wrap-operation
   (fn [op] (fn [x] (conj (op (conj x :e3)) :x3)))})

(def plugin-env
  (p.plugin/register-plugin
    op-plugin))

(deftest register-plugin-test
  (let [plugin (assoc op-plugin ::p.plugin/id 'foo)]
    (is (= (p.plugin/register-plugin
             plugin)
           {:com.wsscode.pathom3.plugin/index-plugins
            {'foo plugin},
            :com.wsscode.pathom3.plugin/plugin-order
            [{::p.plugin/id 'foo}],
            :com.wsscode.pathom3.plugin/plugin-actions
            {:com.wsscode.pathom3.plugin-test/wrap-operation
             [(::wrap-operation plugin)]}})))

  (testing "throw error on duplicated name"
    (is (thrown-with-msg?
          #?(:clj AssertionError :cljs js/Error)
          #"Tried to add duplicated plugin: com.wsscode.pathom3.plugin-test/op-plugin"
          (p.plugin/register [op-plugin op-plugin])))))

(deftest run-with-plugins-test
  (is (= (p.plugin/run-with-plugins plugin-env
           ::unavailable (fn [x] (conj x "S")) ["O"])
         ["O" "S"]))

  (is (= (p.plugin/run-with-plugins plugin-env
           ::wrap-operation (fn [x] (conj x "S")) ["O"])
         ["O" :e1 "S" :x1]))

  (is (= (p.plugin/run-with-plugins (p.plugin/register plugin-env op-plugin2)
           ::wrap-operation (fn [x] (conj x "S")) ["O"])
         ["O" :e1 :e2 "S" :x2 :x1]))

  (is (= (p.plugin/run-with-plugins (-> plugin-env
                                        (p.plugin/register [op-plugin2
                                                            [op-plugin3]]))
           ::wrap-operation (fn [x] (conj x "S")) ["O"])
         ["O" :e1 :e2 :e3 "S" :x3 :x2 :x1]))

  (testing "add in order"
    (is (= (p.plugin/run-with-plugins (-> plugin-env
                                          (p.plugin/register op-plugin2)
                                          (p.plugin/register-before `op-plugin op-plugin3))
             ::wrap-operation (fn [x] (conj x "S")) ["O"])
           ["O" :e3 :e1 :e2 "S" :x2 :x1 :x3]))

    (is (= (p.plugin/run-with-plugins (-> plugin-env
                                          (p.plugin/register op-plugin2)
                                          (p.plugin/register-after `op-plugin op-plugin3))
             ::wrap-operation (fn [x] (conj x "S")) ["O"])
           ["O" :e1 :e3 :e2 "S" :x2 :x3 :x1])))

  (testing "remove plugin"
    (is (= (p.plugin/run-with-plugins (-> plugin-env
                                          (p.plugin/register op-plugin2)
                                          (p.plugin/remove-plugin `op-plugin))
             ::wrap-operation (fn [x] (conj x "S")) ["O"])
           ["O" :e2 "S" :x2]))))
