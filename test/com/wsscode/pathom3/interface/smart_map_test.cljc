(ns com.wsscode.pathom3.interface.smart-map-test
  (:require
    [clojure.core.protocols :as d]
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.built-in.resolvers :as pbir]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.entity-tree :as p.ent]
    [com.wsscode.pathom3.interface.smart-map :as psm]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]
    [com.wsscode.pathom3.test.helpers :as th]
    [matcher-combinators.test])
  #?(:clj
     (:import
       (clojure.lang
         ExceptionInfo))))

(pco/defresolver points-vector []
  {::points-vector
   [{:x 1 :y 10}
    {:x 3 :y 11}
    {:x -10 :y 30}]})

(pco/defresolver points-set []
  {::points-set
   #{{:x 1 :y 10}
     {:x 3 :y 11}
     {:x -10 :y 30}}})

(def registry
  [geo/registry geo/geo->svg-registry
   points-vector points-set])

(deftest smart-map-test
  (testing "reading"
    (testing "keyword call read"
      (let [sm (psm/smart-map (pci/register geo/registry)
                 {::geo/left 3 ::geo/width 5})]
        (is (= (::geo/right sm) 8))))

    (testing "get"
      (let [sm (psm/smart-map (pci/register geo/registry)
                 {::geo/left 3 ::geo/width 5})]
        (is (= (get sm ::geo/right) 8))))

    (testing "calling smart map as a fn"
      (let [sm (psm/smart-map (pci/register geo/registry)
                 {::geo/left 3 ::geo/width 5})]
        (is (= (sm ::geo/right) 8)))))

  (testing "assoc uses source context on the new smart map"
    (let [sm (psm/smart-map (pci/register registry)
               {:x 3 :width 5})]
      (is (= (:right sm) 8))
      (is (= (:right (assoc sm :width 10)) 13)))

    (testing "via conj"
      (let [sm (psm/smart-map (pci/register registry)
                 {:x 3 :width 5})]
        (is (= (:right sm) 8))
        (is (= (:right (conj sm [:width 10])) 13)))))

  (testing "dissoc"
    (let [sm (psm/smart-map (pci/register registry)
               {:x 3 :width 5})]
      (is (= (:right sm) 8))
      (is (= (:right (dissoc sm :width)) nil))))

  (testing "nested maps should also be smart maps"
    (let [sm (psm/smart-map (pci/register registry)
               {:x 10 :y 20})]
      (is (= (-> sm ::geo/turn-point :right)
             10))))

  (testing "nested smart maps should return as-is"
    (let [sm-child (psm/smart-map (pci/register registry) {:x 10 :width 20})
          sm       (psm/smart-map {} {:thing sm-child})]
      (is (= (-> sm :thing :right)
             30))))

  (testing "nested maps in sequences should also be smart maps"
    (testing "vector"
      (let [sm (psm/smart-map (pci/register registry)
                 {})]
        (is (vector? (->> sm ::points-vector)))
        (is (= (->> sm ::points-vector (map :left))
               [1 3 -10]))))

    (testing "set"
      (let [sm (psm/smart-map (pci/register registry)
                 {})]
        (is (= (->> sm ::points-set)
               #{{:x 1 :y 10}
                 {:x 3 :y 11}
                 {:x -10 :y 30}}))
        (is (= (->> sm ::points-set first :left)
               3)))))

  (testing "disable wrap nested"
    (let [sm (psm/smart-map (-> (pci/register registry)
                                (psm/with-wrap-nested? false))
               {:x 10 :y 20})]
      (is (= (-> sm ::geo/turn-point)
             {::geo/bottom 20
              ::geo/right  10}))
      (is (= (-> sm ::geo/turn-point :right)
             nil))))

  (testing "meta"
    (let [sm (-> (pci/register registry)
                 (psm/smart-map {:x 3 :width 5})
                 (with-meta {:foo "bar"}))]
      (is (= (:right sm) 8))
      (is (= (meta sm) {:foo "bar"}))))

  (testing "empty"
    (testing "retains meta"
      (let [sm (-> (pci/register registry)
                   (psm/smart-map {:x 3 :width 5})
                   (with-meta {:foo "bar"})
                   (empty))]
        (is (= sm {}))
        (is (= (-> sm (assoc :x 10)
                   ::geo/x) 10))
        (is (= (meta sm) {:foo "bar"})))))

  (testing "count, uses the count from cache-tree"
    (let [sm (-> (pci/register registry)
                 (psm/smart-map {:x 3 :width 5}))]
      (is (= (:right sm) 8))
      (is (= (count sm) 7))))

  (testing "keys"
    (testing "using cached keys"
      (let [sm (-> (pci/register registry)
                   (psm/smart-map {:x 3 :width 5}))]
        (is (= (:right sm) 8))
        (is (= (into #{} (keys sm))
               #{:x
                 :width
                 :right
                 ::geo/x
                 ::geo/left
                 ::geo/width
                 ::geo/right}))))

    (testing "using reachable keys"
      (let [sm (-> (pci/register geo/full-registry)
                   (psm/with-keys-mode ::psm/keys-mode-reachable)
                   (psm/smart-map {:x 3}))]
        (is (= (into #{} (keys sm))
               #{:com.wsscode.pathom3.test.geometry-resolvers/x
                 :com.wsscode.pathom3.test.geometry-resolvers/left
                 :x
                 :left}))

        (testing "it should not realize the values just by asking the keys"
          (is (= (-> sm psm/sm-env p.ent/entity)
                 {:x 3})))

        (testing "realizing via sequence"
          (is (= (into {} sm)
                 {:com.wsscode.pathom3.test.geometry-resolvers/left 3
                  :com.wsscode.pathom3.test.geometry-resolvers/x    3
                  :left                                             3
                  :x                                                3}))))))

  (testing "contains"
    (testing "using cached keys"
      (let [sm (-> (pci/register registry)
                   (psm/smart-map {:x 3 :width 5}))]
        (is (true? (contains? sm :x)))
        (is (true? (contains? sm :width)))
        (is (false? (contains? sm :wrong)))
        ; only works on CLJ for now, the reason is that contains? on CLJS doens't
        ; take the -contains-key? interface into account, so it's currently not possible
        ; to override the original behavior, which is to do a `get` in the map.
        ; https://clojure.atlassian.net/browse/CLJS-3283
        #?(:clj (is (false? (contains? sm ::geo/x))))))

    (testing "using reachable keys"
      (let [sm (-> (pci/register registry)
                   (psm/with-keys-mode ::psm/keys-mode-reachable)
                   (psm/smart-map {:x 3 :width 5}))]
        (is (true? (contains? sm :x)))
        (is (true? (contains? sm :width)))
        (is (true? (contains? sm ::geo/x))))))

  (testing "seq"
    (let [sm (-> (pci/register registry)
                 (psm/smart-map {:x 3 :width 5}))]
      (is (= (seq sm) [[:x 3] [:width 5]])))

    (testing "nil when keys are empty"
      (let [sm (-> (pci/register registry)
                   (psm/smart-map {}))]
        (is (nil? (seq sm))))))

  (testing "find"
    (let [sm (-> (pci/register registry)
                 (psm/smart-map {:x 3 :width 5}))]
      (is (= (find sm :x) [:x 3])))

    (let [sm (-> (pci/register registry)
                 (psm/smart-map {:not-in-index 42}))]
      (is (= (find sm :not-in-index) [:not-in-index 42])))

    (let [sm (-> (pci/register registry)
                 (psm/smart-map {:x 3 :width 5}))]
      (is (= (find sm :right) [:right 8]))
      (is (= (find sm ::noop) nil)))))

(deftest smart-map-resolver-cache-test
  (testing "uses persistent resolver cache by default"
    (let [spy (th/spy {:return {:foo "bar"}})
          env (pci/register (pco/resolver 'spy {::pco/output [:foo]} spy))
          sm  (psm/smart-map env {})]
      (is (= [(:foo sm)
              (:foo (assoc sm :some "data"))
              (-> spy meta :calls deref count)]
             ["bar" "bar" 1]))))

  (testing "disable persistent cache"
    (let [spy (th/spy {:return {:foo "bar"}})
          env (pci/register {::psm/persistent-cache? false}
                            (pco/resolver 'spy {::pco/output [:foo]} spy))
          sm  (psm/smart-map env {})]
      (is (= [(:foo sm)
              (:foo (assoc sm :some "data"))
              (-> spy meta :calls deref count)]
             ["bar" "bar" 2]))))

  (testing "disabling persistent cache entirely"
    (let [spy (th/spy {:return {:foo "bar"}})
          env (pci/register {:com.wsscode.pathom3.connect.runner/resolver-cache* nil}
                            (pco/resolver 'spy {::pco/output [:foo]} spy))
          sm  (psm/smart-map env {})]
      (is (= [(:foo sm)
              (:foo (assoc sm :some "data"))
              (-> spy meta :calls deref count)]
             ["bar" "bar" 2])))))

(pco/defresolver error-resolver []
  {:error (throw (ex-info "Error" {}))})

(deftest smart-map-datafy-test
  (let [sm  (-> (pci/register [(pbir/alias-resolver :id :name)
                               (pbir/alias-resolver :id :age)])
                (psm/smart-map {:id 10}))
        smd (d/datafy sm)]
    (is (= smd
           {:id   10
            :name ::pco/unknown-value
            :age  ::pco/unknown-value}))

    (testing "navigates in"
      (is (= (d/nav smd :name ::pco/unknown-value)
             10)))))

(deftest smart-map-errors-test
  (testing "quiet mode (default)"
    (let [sm (-> (pci/register error-resolver)
                 (psm/smart-map))]
      (is (= (:error sm) nil))
      (is (= (-> sm
                 (psm/sm-update-env pci/register (pbir/constantly-resolver :not-here "now it is"))
                 :not-here) "now it is"))))

  (testing "loud mode"
    (let [sm (-> (pci/register error-resolver)
                 (psm/with-error-mode ::psm/error-mode-loud)
                 (psm/smart-map))]
      (is (thrown?
            #?(:clj ExceptionInfo :cljs js/Error)
            (:error sm))))

    (testing "planning error"
      (is (thrown?
            #?(:clj ExceptionInfo :cljs js/Error)
            (let [m (psm/smart-map (-> (pci/register
                                         (pbir/alias-resolver :x :y))
                                       (psm/with-error-mode ::psm/error-mode-loud))
                      {})]
              (:y m)))))))

(deftest sm-update-env-test
  (let [sm (-> (pci/register registry)
               (psm/smart-map {:x 3 :width 5}))]
    (is (= (:not-here sm) nil))
    (is (= (-> sm
               (psm/sm-update-env pci/register (pbir/constantly-resolver :not-here "now it is"))
               :not-here) "now it is"))))

(deftest sm-assoc!-test
  (testing "uses source context on the new smart map"
    (let [sm (psm/smart-map (pci/register registry)
               {:x 3 :width 5})]
      (is (= (:right sm) 8))
      (is (= (:right (psm/sm-assoc! sm :width 10)) 8))
      (is (= (:width sm) 10)))))

(deftest sm-dissoc!-test
  (testing "uses source context on the new smart map"
    (let [sm (psm/smart-map (pci/register registry)
               {:x 3 :width 5})]
      (is (= (:right sm) 8))
      (is (= (:right (psm/sm-dissoc! sm :width)) 8)))))

(deftest sm-touch-test
  (testing "loads data from a EQL expression into the smart map"
    (let [sm (-> (psm/smart-map (pci/register registry)
                   {:x 3 :y 5})
                 (psm/sm-touch! [{::geo/turn-point [:right]}]))]
      (is (= (-> sm psm/sm-env p.ent/entity)
             {:x               3
              :y               5
              ::geo/x          3
              ::geo/left       3
              ::geo/y          5
              ::geo/top        5
              ::geo/turn-point {::geo/right  3
                                ::geo/bottom 5
                                :right       3}})))))

(deftest sm-replace-context-test
  (let [sm (-> (psm/smart-map (pci/register registry)
                 {:x 3 :y 5})
               (psm/sm-replace-context {:x 10}))]
    (is (= sm {:x 10}))
    (is (= (::geo/left sm) 10))))
