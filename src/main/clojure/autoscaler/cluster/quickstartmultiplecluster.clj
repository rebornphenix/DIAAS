(ns autoscaler.cluster.quickstartmultiplecluster
  (:use [autoscaler.client.client]
        [autoscaler.cluster.helix]
        [autoscaler.cluster.helixmultiplecluster]
        [autoscaler.keys]
        [autoscaler.utils :as utils]
        [autoscaler.log]))


(defn quickStart [^String connectString]
  (createGrandCluster connectString)
  (createGrandController connectString (createInstanceConfig "localhost" 11000))
  (createGrandController connectString (createInstanceConfig "localhost" 11001))
  (createGrandController connectString (createInstanceConfig "localhost" 11002))
  (let [client (createCuratorFramework connectString)
        manager (singleHelixManager connectString)
        clusterName DEFAULT_TEST_HELIX_CLUSTER_NAME]
    (init manager clusterName)
    (doto manager
      (rebalance clusterName 4))
    (singleHelixAgent client connectString clusterName "localhost" 12000)
    (utils/sleep 1000)
    (singleHelixAgent client connectString clusterName "localhost" 12001)
    (utils/sleep 1000)
    (singleHelixAgent client connectString clusterName "localhost" 12002)
    (utils/sleep 1000)
    (singleHelixAgent client connectString clusterName "localhost" 12003)
    ))
