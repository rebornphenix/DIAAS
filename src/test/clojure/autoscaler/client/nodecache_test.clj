(ns autoscaler.client.nodecache-test
  (:require [clojure.test :refer :all]
            [autoscaler.client.nodecache :refer :all]
            )
  (:use [autoscaler.utils]
        [autoscaler.client.client :only [singleCuratorFramework]])
  (:import (org.apache.curator.test TestingServer)))

(defn getNodeCache [path]
  (let [connectString (.getConnectString (TestingServer.))
        client (singleCuratorFramework connectString)
        nodeCache (singleNodeCache client path "test")]
    nodeCache))

(deftest try-nodecache-test
  (testing "Get default value from Node Cache"
    (let [nodeCache (getNodeCache "/test_nodecache_path1")]
      (sleep 1000)
      (is (.equals (getValue nodeCache) "test"))))
  (testing "Get set value from Node Cache"
    (let [nodeCache (getNodeCache "/test_nodecache_path2") expectedValue "test1"]
      (setValue nodeCache expectedValue)
      (sleep 1000)
      (is (.equals (getValue nodeCache) expectedValue)))))

