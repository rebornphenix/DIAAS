(ns autoscaler.cluster.quickstartscaleout
  (:require
    [autoscaler.scheduler :as scheduler]
    [autoscaler.utils :refer [sleep]])
  (:use
    [autoscaler.client.client]
    [autoscaler.cluster.helix]
    [autoscaler.cluster.helixmultiplecluster]
    [autoscaler.keys]
    ))

(defn quickStartScaleOut [^String connectString]
  (createGrandCluster connectString)
  (singleHelixController connectString "192.168.0.110")
  (singleHelixController connectString "192.168.0.111")
  (singleHelixController connectString "192.168.0.112")
  (let [resourceName DEFAULT_HELIX_RESOURCE_NAME
        manager (singleHelixManager connectString resourceName)
        clusterName DEFAULT_TEST_HELIX_CLUSTER_NAME]
    (init manager clusterName)
    (singleHelixAdministrator connectString resourceName clusterName "192.168.1.130")
    (doto manager
      (rebalance clusterName 4))
    (scheduler/startScheduler connectString)
    (sleep 180000)
    (doto manager
      (rebalance clusterName 8))
    (sleep 180000)
    (doto manager
      (rebalance clusterName 4))
    ))
