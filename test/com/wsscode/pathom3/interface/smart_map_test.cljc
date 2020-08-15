(ns com.wsscode.pathom3.interface.smart-map-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.interface.smart-map :as psm]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]))

(pco/defresolver points-vector [] ::points-vector
  [{:x 1 :y 10}
   {:x 3 :y 11}
   {:x -10 :y 30}])

(pco/defresolver points-set [] ::points-set
  #{{:x 1 :y 10}
    {:x 3 :y 11}
    {:x -10 :y 30}})

(def registry
  [geo/registry geo/geo->svg-registry
   points-vector points-set])

(deftest smart-map-test
  (testing "reading"
    (testing "get"
      (let [sm (psm/smart-map (pci/register geo/registry)
                 {::geo/left 3 ::geo/width 5})]
        (is (= (get sm ::geo/right) 8))))

    (testing "keyword call read"
      (let [sm (psm/smart-map (pci/register geo/registry)
                 {::geo/left 3 ::geo/width 5})]
        (is (= (::geo/right sm) 8))))

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

  (testing "nested maps in sequences should also be smart maps"
    (testing "vector"
      (let [sm (psm/smart-map (pci/register registry)
                 {})]
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

  (testing "meta"
    (let [sm (-> (pci/register registry)
                 (psm/smart-map {:x 3 :width 5})
                 (with-meta {:foo "bar"}))]
      (is (= (:right sm) 8))
      (is (= (meta sm) {:foo "bar"}))))

  (testing "count, uses the count from cache-tree"
    (let [sm (-> (pci/register registry)
                 (psm/smart-map {:x 3 :width 5}))]
      (is (= (:right sm) 8))
      (is (= (count sm) 7))))

  (testing "keys, uses the count from cache-tree"
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

  (testing "find"
    (let [sm (-> (pci/register registry)
                 (psm/smart-map {:x 3 :width 5}))]
      (is (= (find sm :right) [:right 8]))
      (is (= (find sm ::noop) nil)))))

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
