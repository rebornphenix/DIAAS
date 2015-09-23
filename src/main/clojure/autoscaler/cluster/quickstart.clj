(ns autoscaler.cluster.quickstart
  (:use [autoscaler.client.client]
        [autoscaler.cluster.helix]
        [autoscaler.keys]
        [autoscaler.utils :as utils]
        [autoscaler.log]))


(defn quickStart [^String connectString]
  (let [client (createCuratorFramework connectString)
        manager (singleHelixManager connectString)
        clusterName DEFAULT_TEST_HELIX_CLUSTER_NAME]
    (init manager clusterName)
    (doto manager
      (rebalance clusterName 4))
    (singleHelixAgent client connectString clusterName "localhost" 12000)
    (createHelixController connectString clusterName)
    (utils/sleep 1000)
    (singleHelixAgent client connectString clusterName "localhost" 12001)
    (utils/sleep 1000)
    (singleHelixAgent client connectString clusterName "localhost" 12002)
    (utils/sleep 1000)
    (singleHelixAgent client connectString clusterName "localhost" 12003)
    ))
