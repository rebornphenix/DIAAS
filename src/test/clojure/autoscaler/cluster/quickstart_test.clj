(ns autoscaler.cluster.quickstart-test
  (:require [clojure.test :refer :all]
            [autoscaler.cluster.quickstart :refer :all]
            )
  (:import (org.apache.curator.test TestingServer)))

(deftest try-quickstart-test
  (testing "Running quickstart"
    (let [connectString (.getConnectString (TestingServer. 2177))]
      (quickStart connectString))
    (is (= 1 1))))
