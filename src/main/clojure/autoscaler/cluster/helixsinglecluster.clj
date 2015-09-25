(ns autoscaler.cluster.helixsinglecluster
  (:import (org.apache.helix.model IdealState$RebalanceMode)
           (org.apache.helix.manager.zk ZKHelixAdmin)
           (org.apache.helix.controller HelixControllerMain))
  (:require [autoscaler.client.client])
  (:use [autoscaler.cluster.model :as model]
        [autoscaler.keys]
        [autoscaler.log]
        [autoscaler.cluster.helix]
        [autoscaler.cluster.zkclientpool :as pool]))

(defprotocol HelixAutoscalerManager
  (init [this clusterName])
  (rebalance [this clusterName rebalanceReplica]))

(defn ^HelixAutoscalerManager createHelixManager [^String connectString]
  (let [client (pool/zkClient connectString)
        admin (ZKHelixAdmin. client)
        resourceName DEFAULT_HELIX_RESOURCE_NAME
        stateModelRef DEFAULT_HELIX_STATE_MODEL_REF
        partitions DEFAULT_HELIX_CLUSTER_PARTITION]
    (reify
      HelixAutoscalerManager
      (init [_ clusterName]
        (ensureClusterNotExist client clusterName)
        (.addCluster admin clusterName)
        (.addStateModelDef admin clusterName stateModelRef (model/defineStateModel stateModelRef))
        (.addResource admin clusterName resourceName partitions stateModelRef (.toString IdealState$RebalanceMode/FULL_AUTO)))
      (rebalance [_ clusterName rebalanceReplica]
        (.rebalance admin clusterName resourceName rebalanceReplica)))))

(def singleHelixManager (memoize createHelixManager))

(defn createHelixController ([^String connectString ^String clusterName] (createHelixController connectString clusterName (generateControllerName clusterName) (HelixControllerMain/STANDALONE)))
  ([^String connectString ^String clusterName ^String controllerName ^String mode]
   (HelixControllerMain/startHelixController connectString clusterName controllerName mode)))


