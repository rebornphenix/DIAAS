(ns autoscaler.client.mondataqueue-test
  (:require [clojure.test :refer :all]
            [autoscaler.client.mondataqueue :refer :all]
            )
  (:use [autoscaler.utils]
        [autoscaler.client.client :only [singleCuratorFramework]])
  (:import (autoscaler.client.mondataqueue MonData)
           (java.util Date)
           (org.apache.curator.test TestingServer)))

(deftest try-parse-mondata-test
  (testing "parse mon data"
    (let [cpuUsage 10.1
          memoryUsage 10.2
          timestamp (Date.)
          clusterName "testClusterName"
          mondata (MonData. cpuUsage memoryUsage timestamp clusterName)
          parsedMondata (parse (objToString mondata))]
      (is (= (:cpuUsage parsedMondata) cpuUsage))
      (is (= (:memoryUsage parsedMondata) memoryUsage))
      (is (= (.getTime (:timestamp parsedMondata)) (.getTime timestamp)))
      (is (.equals (:clusterName parsedMondata) clusterName))
      )))

(defn getMonDataQueue [path]
  (let [connectString (.getConnectString (TestingServer.))
        client (singleCuratorFramework connectString)
        monDataQueue (singleMonDataQueue client path)]
    monDataQueue))

(deftest try-mondata-usage-test
  (testing "parse mon data"
    (let [cpuUsage 10.1
          memoryUsage 10.2
          timestamp (Date.)
          clusterName "testClusterName"
          mondata (MonData. cpuUsage memoryUsage timestamp clusterName)
          monDataQueue (getMonDataQueue "/test-mondata-queue")]
      (putData monDataQueue mondata)
      (sleep 1000)
      (let [data (first (getData monDataQueue))]
        (is (= (:cpuUsage data) cpuUsage))
        (is (= (:memoryUsage data) memoryUsage))
        (is (= (.getTime (:timestamp data)) (.getTime timestamp)))
        (is (.equals (:clusterName data) clusterName))
        )
      (cleanUp monDataQueue)
      (let [data (getData monDataQueue)]
        (is (empty? data)))
      )
    ))

