(ns autoscaler.client.client-test
  (:require [clojure.test :refer :all]
            [autoscaler.client.client :refer :all]
            )
  (:import (org.apache.curator.test TestingServer)))

(deftest try-curatorframework-test
  (testing "add child and get children"
    (let [connectString (.getConnectString (TestingServer.))
          client (singleCuratorFramework connectString)
          path "/testchildpath"
          child "child1"]
      (is (empty? (getChildren client path)))
      (addChild client path child)
      (is (.equals (first (getChildren client path)) child))))
  (testing "set data and get data"
    (let [connectString (.getConnectString (TestingServer.))
          client (singleCuratorFramework connectString)
          path "/testdatapath"
          data "data1"]
      (is (empty? (getData client path)))
      (setData client path data)
      (is (.equals (getData client path) data))))
  (testing "check the exit of the path"
    (let [connectString (.getConnectString (TestingServer.))
          client (singleCuratorFramework connectString)
          path "/testcheckexistpath"
          data "data1"]
      (is (not (exist client path)))
      (addChild client path "testpath")
      (is (exist client path))))
  )



