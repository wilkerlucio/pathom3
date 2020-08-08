(ns com.wsscode.pathom3.interface.smart-map-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.pathom3.connect.indexes :as pci]
    [com.wsscode.pathom3.connect.operation :as pco]
    [com.wsscode.pathom3.interface.smart-map :as psm]
    [com.wsscode.pathom3.test.geometry-resolvers :as geo]))

(pco/defresolver points [] ::points
  [{:x 1 :y 10}
   {:x 3 :y 11}
   {:x -10 :y 30}])

(deftest smart-map-test
  (let [sm (psm/smart-map
             (pci/register geo/registry)
             {::geo/left 3 ::geo/width 5})]
    (is (= (::geo/right sm) 8)))

  (testing "assoc uses source context on the new smart map"
    (let [sm (psm/smart-map
               (pci/register [geo/registry geo/geo->svg-registry])
               {:x 3 :width 5})]
      (is (= (:right sm) 8))
      (is (= (:right (assoc sm :width 10)) 13))))

  (testing "nested maps should also be smart maps"
    (let [sm (psm/smart-map
               (pci/register [geo/registry geo/geo->svg-registry])
               {:x 10 :y 20})]
      (is (= (-> sm ::geo/turn-point :right)
             10))))

  (testing "nested maps in sequences should also be smart maps"
    (let [sm (psm/smart-map
               (pci/register [geo/registry geo/geo->svg-registry
                              points])
               {})]
      (is (= (->> sm ::points (map :left))
             [1 3 -10])))))

(deftest sm-assoc!-test
  (testing "assoc uses source context on the new smart map"
    (let [sm (psm/smart-map
               (pci/register [geo/registry geo/geo->svg-registry])
               {:x 3 :width 5})]
      (is (= (:right sm) 8))
      (is (= (:right (psm/sm-assoc! sm :width 10)) 8)))))
