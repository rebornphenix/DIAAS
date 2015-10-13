(ns autoscaler.cluster.quickstartmultiplecluster
  (:use [autoscaler.client.client]
        [autoscaler.cluster.helix]
        [autoscaler.cluster.helixmultiplecluster]
        [autoscaler.keys]
        [autoscaler.utils :as utils]
        [autoscaler.log]))


(defn quickStartMultiple [^String connectString]
  (createGrandCluster connectString)
  (singleHelixController connectString "192.168.1.110")
  (singleHelixController connectString "192.168.1.111")
  (singleHelixController connectString "192.168.1.112")
  (let [client (singleCuratorFramework connectString)
        resoureName DEFAULT_HELIX_RESOURCE_NAME
        manager (singleHelixManager connectString resoureName)
        clusterName DEFAULT_TEST_HELIX_CLUSTER_NAME]
    (init manager clusterName)
    (singleHelixAdministrator connectString resoureName clusterName "192.168.1.130")
    (singleHelixAgent client connectString clusterName "192.168.1.120")
    (utils/sleep 1000)
    (singleHelixAgent client connectString clusterName "192.168.1.121")
    (utils/sleep 1000)
    (singleHelixAgent client connectString clusterName "192.168.1.122")
    (utils/sleep 1000)
    (singleHelixAgent client connectString clusterName "192.168.1.123")
    (doto manager
      (rebalance clusterName 4))
    (utils/sleep 90000)
    (setAgentStateOffline connectString clusterName "192.168.1.121")
    (utils/sleep 90000)
    (setAgentStateOnline connectString clusterName "192.168.1.121")
    (utils/sleep 90000)
    (setClusterStateOffline connectString clusterName)
    (utils/sleep 90000)
    (setClusterStateOnline connectString clusterName)
    ))
