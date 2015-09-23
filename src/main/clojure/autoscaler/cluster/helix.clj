(ns autoscaler.cluster.helix
  (:import (org.apache.helix.model InstanceConfig IdealState$RebalanceMode)
           (org.apache.helix HelixManagerFactory InstanceType)
           (org.apache.helix.manager.zk ZKHelixAdmin HelixManagerShutdownHook)
           (org.apache.curator.framework CuratorFramework)
           (java.util UUID)
           (org.apache.helix.controller HelixControllerMain))
  (:require [autoscaler.client.client])
  (:use [autoscaler.cluster.model :as model]
        [autoscaler.keys]
        [autoscaler.log]))

(defn createInstanceConfig [^String host ^long port]
  (let [instance (InstanceConfig. (str host "_" (String/valueOf port)))]
    (doto instance
      (.setHostName host)
      (.setPort (String/valueOf port))
      (.setInstanceEnabled true))
    instance))

(defprotocol HelixAutoscalerManager
  (init [this clusterName])
  (addInstanceConfig [this clusterName instanceConfig])
  (rebalance [this clusterName rebalanceReplica]))

(defn ^HelixAutoscalerManager createHelixManager [^String connectString]
  (let [admin (ZKHelixAdmin. connectString)
        resourceName DEFAULT_HELIX_RESOURCE_NAME
        stateModelRef DEFAULT_HELIX_STATE_MODEL_REF
        partitions DEFAULT_HELIX_CLUSTER_PARTITION]
    (reify
      HelixAutoscalerManager
      (init [_ clusterName]
        (.addCluster admin clusterName)
        (.addStateModelDef admin clusterName stateModelRef (model/defineStateModel stateModelRef))
        (.addResource admin clusterName resourceName partitions stateModelRef (.toString IdealState$RebalanceMode/FULL_AUTO)))
      (addInstanceConfig [_ clusterName instanceConfig]
        (.addInstance admin clusterName instanceConfig))
      (rebalance [_ clusterName rebalanceReplica]
        (.rebalance admin clusterName resourceName rebalanceReplica)))))

(def singleHelixManager (memoize createHelixManager))

(defn ^String generateControllerName [^String clusterName]
  (str clusterName "-" (.toString (UUID/randomUUID))))

(defn createHelixController ([^String connectString ^String clusterName] (createHelixController connectString clusterName (generateControllerName clusterName) (HelixControllerMain/STANDALONE)))
  ([^String connectString ^String clusterName ^String controllerName ^String mode]
   (HelixControllerMain/startHelixController connectString clusterName controllerName mode)))

(defprotocol HelixAutoscalerAgent
  (start [this clusterName]))

(defn- createHelixAutoscalerAgent [^CuratorFramework client ^String connectString ^String stateModelDef ^InstanceConfig instance ^String hostIp]
  (reify HelixAutoscalerAgent
    (start [_ clusterName]
      (let [admin (ZKHelixAdmin. connectString)
            instanceName (.getId instance)
            manager (HelixManagerFactory/getZKHelixManager clusterName instanceName (InstanceType/PARTICIPANT) connectString)]
        (.addInstance admin clusterName instance)
        (.registerStateModelFactory (.getStateMachineEngine manager) stateModelDef (createStateModelFactory client instanceName hostIp))
        (.connect manager)
        (.addShutdownHook (Runtime/getRuntime) (HelixManagerShutdownHook. manager))
        (log-message (str instanceName " has been successfully started!"))))))

(defn ^HelixAutoscalerAgent createHelixAgent
  ([^CuratorFramework client ^String connectString ^String clusterName ^String hostIp]
   (createHelixAgent client connectString clusterName hostIp DEFAULT_AGENT_PORT))
  ([^CuratorFramework client ^String connectString ^String clusterName ^String hostIp port]
   (let [agent (createHelixAutoscalerAgent client connectString DEFAULT_HELIX_STATE_MODEL_REF (createInstanceConfig hostIp port) hostIp)]
     (.start agent clusterName)
     agent)))

(def singleHelixAgent (memoize createHelixAgent))


