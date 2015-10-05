(ns autoscaler.cluster.quickstartmultiplecluster
  (:use [autoscaler.client.client]
        [autoscaler.cluster.helix]
        [autoscaler.cluster.helixmultiplecluster]
        [autoscaler.keys]
        [autoscaler.utils :as utils]
        [autoscaler.log]))


(defn quickStartMultiple [^String connectString]
  (createGrandCluster connectString)
  (createGrandController connectString (createInstanceConfig "192.168.1.110" 11000))
  (createGrandController connectString (createInstanceConfig "192.168.1.111" 11000))
  (createGrandController connectString (createInstanceConfig "192.168.1.112" 11000))
  (let [client (createCuratorFramework connectString)
        manager (singleHelixManager connectString)
        clusterName DEFAULT_TEST_HELIX_CLUSTER_NAME]
    (init manager clusterName)
    (doto manager
      (rebalance clusterName 4))
    (singleHelixAgent client connectString clusterName "192.168.1.120")
    (utils/sleep 1000)
    (singleHelixAgent client connectString clusterName "192.168.1.121")
    (utils/sleep 1000)
    (singleHelixAgent client connectString clusterName "192.168.1.122")
    (utils/sleep 1000)
    (singleHelixAgent client connectString clusterName "192.168.1.123")
    (utils/sleep 90000)
    (setAgentStateOffline connectString clusterName "192.168.1.121")
    (utils/sleep 90000)
    (setAgentStateOnline connectString clusterName "192.168.1.121")
    (utils/sleep 90000)
    (setClusterStateOffline connectString clusterName)
    (utils/sleep 90000)
    (setClusterStateOnline connectString clusterName)
    ))
