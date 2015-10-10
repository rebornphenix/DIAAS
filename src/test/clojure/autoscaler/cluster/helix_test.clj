(ns autoscaler.cluster.helix-test
  (:require [clojure.test :refer :all]
            [autoscaler.cluster.helix :refer :all]
            )
  (:import (org.apache.curator.test TestingServer)))

(deftest try-set-cluster-status-test
  (testing "Get default value"
    (let [clusterName "clusterName"
          connectString (.getConnectString (TestingServer.))]
      (is (= (getClusterTransitionStatus connectString clusterName) false))))
  (testing "Get prepopulated value"
    (let [clusterName "clusterName"
          connectString (.getConnectString (TestingServer.))
          value true]
      (setClusterTransitionStatus connectString clusterName value)
      (is (= (getClusterTransitionStatus connectString clusterName) value)))))


(deftest try-set-cluster-ideal-size-test
  (testing "Get default value"
    (let [clusterName "clusterName"
          connectString (.getConnectString (TestingServer.))]
      (is (= (getClusterIdealSize connectString clusterName) 0))))
  (testing "Get prepopulated value"
    (let [clusterName "clusterName"
          connectString (.getConnectString (TestingServer.))
          value 10]
      (setClusterIdealSize connectString clusterName value)
      (is (= (getClusterIdealSize connectString clusterName) value)))))