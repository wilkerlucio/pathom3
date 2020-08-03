(ns com.wsscode.misc.core-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.misc.core :as misc]))

(deftest cljc-random-uuid-test
  (is (uuid? (misc/cljc-random-uuid))))

(deftest distinct-by-test
  (is (= (misc/distinct-by :id
                           [{:id   1
                             :name "foo"}
                            {:id   2
                             :name "bar"}
                            {:id   1
                             :name "other"}])
         [{:id   1
           :name "foo"}
          {:id   2
           :name "bar"}])))

(deftest dedupe-by-test
  (is (= (misc/dedupe-by :id
                         [{:id   1
                           :name "foo"}
                          {:id   1
                           :name "dedup-me"}
                          {:id   2
                           :name "bar"}
                          {:id   1
                           :name "other"}])
         [{:id   1
           :name "foo"}
          {:id   2
           :name "bar"}
          {:id   1
           :name "other"}])))

(deftest index-by-test
  (is (= (misc/index-by :id
                        [{:id   1
                          :name "foo"}
                         {:id   1
                          :name "dedup-me"}
                         {:id   2
                          :name "bar"}
                         {:id   1
                          :name "other"}])
         {1 {:id 1, :name "other"}, 2 {:id 2, :name "bar"}})))

(deftest sconj-test
  (is (= (misc/sconj nil 42) #{42}))
  (is (set? (misc/sconj nil 42))))

(deftest vconj-test
  (is (= (misc/vconj nil 42) [42]))
  (is (vector? (misc/vconj nil 42))))

(deftest queue-test
  (let [queue (-> (misc/queue)
                  (conj 1 2))]
    (is (= queue [1 2]))
    (is (= (peek queue) 1))
    (is (= (pop queue) [2])))

  (let [queue (misc/queue [1 2])]
    (is (= queue [1 2]))
    (is (= (peek queue) 1))
    (is (= (pop queue) [2]))))

(deftest map-keys-test
  (is (= (misc/map-keys inc {1 :a 2 :b})
         {2 :a 3 :b})))

(deftest map-vals-test
  (is (= (misc/map-vals inc {:a 1 :b 2})
         {:a 2 :b 3})))

(deftest atom?-test
  (is (true? (misc/atom? (atom "x"))))
  (is (false? (misc/atom? "x"))))

(deftest merge-grow-test
  (is (= (misc/merge-grow) {}))
  (is (= (misc/merge-grow {:foo "bar"}) {:foo "bar"}))

  (testing "merge sets by union"
    (is (= (misc/merge-grow {:foo #{:a}} {:foo #{:b}})
           {:foo #{:a :b}})))

  (testing "merge maps"
    (is (= (misc/merge-grow {:foo {:a 1}} {:foo {:b 2}})
           {:foo {:a 1 :b 2}})))

  (testing "keep left value if right one is nil"
    (is (= (misc/merge-grow {:foo {:a 1}} {:foo {:a nil}})
           {:foo {:a 1}}))))
