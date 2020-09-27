(ns com.wsscode.misc.math-test
  (:require
    [clojure.test :refer [deftest is are run-tests testing]]
    [com.wsscode.misc.math :as math]))

(deftest round-test
  (is (= (math/round 30.2) 30)))
