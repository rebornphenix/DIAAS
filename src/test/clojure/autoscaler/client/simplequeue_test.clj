(ns autoscaler.client.simplequeue-test
  (:require [clojure.test :refer :all]
            [autoscaler.client.client :refer :all]
            [autoscaler.client.simplequeue :refer :all]
            )
  (:use [autoscaler.utils])
  (:import (org.apache.curator.test TestingServer))
  )

(defn getSimpleQueue [path]
  (let [connectString (.getConnectString (TestingServer.))
        client (singleCuratorFramework connectString)
        simpleQueue (singleSimpleQueue client path)]
    simpleQueue))

(deftest try-simplequeue-test
  (testing "take value"
    (let [queue (getSimpleQueue "/test_simplequeue_path1") expectedValue "test"]
      (soffer queue expectedValue)
      (is (.equals (stake queue) expectedValue))))
  (testing "peek value"
    (let [queue (getSimpleQueue "/test_simplequeue_path2") expectedValue "test1"]
      (soffer queue expectedValue)
      (sleep 1000)
      (is (.equals (speek queue) expectedValue))))
  (testing "remove value"
    (let [queue (getSimpleQueue "/test_simplequeue_path3") expectedValue "test2"]
      (soffer queue expectedValue)
      (sleep 1000)
      (is (.equals (sremove queue) expectedValue))
      (is (nil? (sremove queue))))))
