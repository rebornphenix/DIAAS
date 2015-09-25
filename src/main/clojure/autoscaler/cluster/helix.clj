(ns autoscaler.cluster.helix
  (:import (org.apache.helix.model InstanceConfig)
           (java.util UUID)
           (org.apache.curator.framework CuratorFramework)
           (org.apache.helix.manager.zk HelixManagerShutdownHook ZKHelixAdmin ZkClient)
           (org.apache.helix InstanceType HelixManagerFactory))

  (:use [autoscaler.log]
        [autoscaler.keys]
        [autoscaler.cluster.model :as model]
        [autoscaler.cluster.zkclientpool :as pool]))

(defn ensureClusterNotExist [^ZkClient client ^String clusterName]
  (let [path (str "/" clusterName)]
    (if (.exists client path)
      (.deleteRecursive client path)
      )))

(defn createInstanceConfig [^String host ^long port]
  (let [instance (InstanceConfig. (str host "_" (String/valueOf port)))]
    (doto instance
      (.setHostName host)
      (.setPort (String/valueOf port))
      (.setInstanceEnabled true))
    instance))

(defn ^String generateControllerName [^String clusterName]
  (str clusterName "-" (.toString (UUID/randomUUID))))

(defprotocol HelixAutoscalerAgent
  (start [this clusterName]))

(defn- createHelixAutoscalerAgent [^CuratorFramework client ^String connectString ^String stateModelDef ^InstanceConfig instance ^String hostIp]
  (reify HelixAutoscalerAgent
    (start [_ clusterName]
      (let [admin (ZKHelixAdmin. (pool/zkClient connectString))
            instanceName (.getId instance)
            manager (HelixManagerFactory/getZKHelixManager clusterName instanceName (InstanceType/PARTICIPANT) connectString)]
        (.addInstance admin clusterName instance)
        (.registerStateModelFactory (.getStateMachineEngine manager) stateModelDef (model/createStateModelFactory client instanceName hostIp))
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