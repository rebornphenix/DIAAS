(ns autoscaler.cluster.quickstartmultiplecluster_test
  (:require [clojure.test :refer :all]
            [autoscaler.cluster.quickstartmultiplecluster :refer :all]
            )
  (:import (org.apache.curator.test TestingServer)))

(deftest try-quickstart-test
  (testing "Running quickstart"
    (let [connectString (.getConnectString (TestingServer. 2178))]
      (quickStartMultiple connectString))
    (is (= 1 1))))
