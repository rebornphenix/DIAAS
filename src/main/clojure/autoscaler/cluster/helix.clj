(ns autoscaler.cluster.helix
  (:import (org.apache.helix.model InstanceConfig ExternalView)
           (java.util UUID)
           (org.apache.curator.framework CuratorFramework)
           (org.apache.helix.manager.zk HelixManagerShutdownHook ZKHelixAdmin ZkClient ZKHelixManager)
           (org.apache.helix InstanceType HelixManagerFactory ExternalViewChangeListener LiveInstanceChangeListener)
           (clojure.lang PersistentTreeMap))

  (:use [autoscaler.log]
        [autoscaler.keys]
        [autoscaler.cluster.model :as model]
        [autoscaler.cluster.zkclientpool :as pool]
        [autoscaler.status]
        [autoscaler.client.lock :as lock]
        [autoscaler.client.client :as client]))

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

(defn- createClusterLockFor [^String connectString ^String clusterName keyFunc]
  (let [client (client/singleCuratorFramework connectString)]
    (lock/singleLock client (keyFunc clusterName))))

(defn- addToBeRemovedToCluster [^String connectString ^String clusterName ^String hostIp]
  (let [client (client/singleCuratorFramework connectString)
        lock (createClusterLockFor connectString clusterName getClusterStatusLockKey)
        path (getClusterToBeRemovedKey clusterName)]
    (run lock (reify Command
                (execute [_]
                  (addChild client path hostIp))))))

(defn deleteToBeRemovedFromCluster [^String connectString ^String clusterName ^String hostIp]
  (let [client (client/singleCuratorFramework connectString)
        lock (createClusterLockFor connectString clusterName getClusterStatusLockKey)
        path (str (getClusterToBeRemovedKey clusterName) "/" hostIp)]
    (run lock (reify Command
                (execute [_]
                  (delete client path))))))

(defn getToBeRemovedsFromCluster [^String connectString ^String clusterName]
  (let [client (client/singleCuratorFramework connectString)
        lock (createClusterLockFor connectString clusterName getClusterStatusLockKey)
        valuePath (getClusterToBeRemovedKey clusterName)
        retValue (ref '())]
    (run lock (reify Command
                (execute [_]
                  (let [value (getChildren client valuePath)]
                    (dosync (alter retValue conj value))))))
    (first (deref retValue))))


(defn- createHelixAutoscalerAgent [^CuratorFramework client ^String connectString ^String stateModelDef ^InstanceConfig instance ^String hostIp]
  (reify HelixAutoscalerAgent
    (start [_ clusterName]
      (let [admin (ZKHelixAdmin. (pool/zkClient connectString))
            instanceName (.getId instance)
            manager (HelixManagerFactory/getZKHelixManager clusterName instanceName (InstanceType/PARTICIPANT) connectString)]
        (.addInstance admin clusterName instance)
        (.registerStateModelFactory (.getStateMachineEngine manager) stateModelDef (model/createStateModelFactory client instanceName hostIp (partial addToBeRemovedToCluster connectString)))
        (.connect manager)
        (.addShutdownHook (Runtime/getRuntime) (HelixManagerShutdownHook. manager))
        (log-message (str instanceName " has been successfully started!"))))))

(defn- ^HelixAutoscalerAgent createHelixAgent
  ([^CuratorFramework client ^String connectString ^String clusterName ^String hostIp]
   (createHelixAgent client connectString clusterName hostIp DEFAULT_AGENT_PORT))
  ([^CuratorFramework client ^String connectString ^String clusterName ^String hostIp port]
   (let [agent (createHelixAutoscalerAgent client connectString DEFAULT_HELIX_STATE_MODEL_REF (createInstanceConfig hostIp port) hostIp)]
     (start agent clusterName)
     agent)))

(def singleHelixAgent (memoize createHelixAgent))

(defn- stateToMessage [isOnline]
  (if isOnline STATUS_ONLINE STATUS_OFFLINE))

(defn- setAgentStateTo [^String connectString ^String clusterName ^String hostIp port isOnline]
  (let [admin (ZKHelixAdmin. (pool/zkClient connectString))
        instanceName (.getId (createInstanceConfig hostIp port))]
    (.enableInstance admin clusterName instanceName isOnline)
    (log-message (str "set host " hostIp ":" port " to " (stateToMessage isOnline)))
    ))

(defn setAgentStateOffline ([^String connectString ^String clusterName ^String hostIp] (setAgentStateOffline connectString clusterName hostIp DEFAULT_AGENT_PORT))
  ([^String connectString ^String clusterName ^String hostIp port]
   (setAgentStateTo connectString clusterName hostIp port false)))

(defn setAgentStateOnline ([^String connectString ^String clusterName ^String hostIp] (setAgentStateOnline connectString clusterName hostIp DEFAULT_AGENT_PORT))
  ([^String connectString ^String clusterName ^String hostIp port]
   (setAgentStateTo connectString clusterName hostIp port true)))

(defn- setClusterStateTo [^String connectString ^String clusterName isOnline]
  (let [admin (ZKHelixAdmin. (pool/zkClient connectString))
        resourceName (first (.getResourcesInCluster admin clusterName))]
    (.enableResource admin clusterName resourceName isOnline)
    (log-message (str "set cluster " clusterName " to " (stateToMessage isOnline)))))

(defn setClusterStateOffline [^String connectString ^String clusterName]
  (setClusterStateTo connectString clusterName false))

(defn setClusterStateOnline [^String connectString ^String clusterName]
  (setClusterStateTo connectString clusterName true))


(defn- setClusterStatus [^String connectString ^String clusterName status lockKeyFunc nodeCacheFunc logFunc]
  (let [client (client/singleCuratorFramework connectString)
        lock (createClusterLockFor connectString clusterName lockKeyFunc)

        valuePath (nodeCacheFunc clusterName)]
    (run lock (reify Command
                (execute [_]
                  (setData client valuePath status)
                  (logFunc))))))

(defn- getClusterStatus [^String connectString ^String clusterName defaultValueFunc lockKeyFunc nodeCacheFunc]
  (let [client (client/singleCuratorFramework connectString)
        lock (createClusterLockFor connectString clusterName lockKeyFunc)
        valuePath (nodeCacheFunc clusterName)
        retValue (ref '())]
    (run lock (reify Command
                (execute [_]
                  (let [value (getDataWithDefaultValue client valuePath (defaultValueFunc))]
                    (dosync (alter retValue conj value))))))
    (first (deref retValue))))


(defn- defaultIdealSize []
  (String/valueOf 0))

(defn- stringToLong [value]
  (if (.equals value "") 0 (Long/valueOf value)))

(defn setClusterIdealSize [^String connectString ^String clusterName ^long size]
  (setClusterStatus connectString clusterName (String/valueOf size) getClusterStatusLockKey getClusterIdealSizeKey
                    #(log-message (str "set the ideal size of the cluster " clusterName " to " (String/valueOf size)))))

(defn getClusterIdealSize [^String connectString ^String clusterName]
  (stringToLong (getClusterStatus connectString clusterName defaultIdealSize getClusterStatusLockKey getClusterIdealSizeKey)))

(defn setClusterCurrentSize [^String connectString ^String clusterName ^long size]
  (setClusterStatus connectString clusterName (String/valueOf size) getClusterStatusLockKey getClusterCurrentSizeKey
                    #(log-message (str "set the current size of the cluster " clusterName " to " (String/valueOf size)))))

(defn getClusterCurrentSize [^String connectString ^String clusterName]
  (stringToLong (getClusterStatus connectString clusterName defaultIdealSize getClusterStatusLockKey getClusterCurrentSizeKey)))


(defn- viewChangeListener-handleParition [^ExternalView view ^String connectString ^String clusterName ^String partition]
  (let [stateMap (PersistentTreeMap/create (.getStateMap view partition))
        currentSize (count (filter #(contains? AGENT_NORMAL_STATUS %) (vals stateMap)))]
    (do
      (log-message "start to print the status of agents")
      (dorun (map #(log-message (str (first %) "---" (last %))) stateMap))
      (log-message "finish printing the status of agents")
      (setClusterCurrentSize connectString clusterName currentSize))))


(defn- viewChangeListener-handleView [^ExternalView view ^String connectString ^String clusterName]
  (dorun (map #(viewChangeListener-handleParition view connectString clusterName %) (.getPartitionSet view))))


(defn- createHelixAdministrator [^String connectString ^String resourceName ^String clusterName ^String hostIp]
  (let [manager (ZKHelixManager. clusterName (.getId (createInstanceConfig hostIp DEFAULT_SPECTATOR_PORT)) (InstanceType/ADMINISTRATOR) connectString)
        liveInstanceChangeListener (proxy [LiveInstanceChangeListener] []
                                     (onLiveInstanceChange [instances _]
                                       (let [client (pool/zkClient connectString)
                                             admin (ZKHelixAdmin. client)
                                             numOfInstances (count instances)
                                             idealSize (getClusterIdealSize connectString clusterName)]
                                         (do
                                           (log-message (str
                                                          "live instance listen ideaSize is "
                                                          (String/valueOf idealSize)
                                                          " and numOfInstances is "
                                                          (String/valueOf numOfInstances)))
                                           (if (== idealSize numOfInstances)
                                             (.rebalance admin clusterName resourceName numOfInstances))))))
        externalViewListener (proxy [ExternalViewChangeListener] []
                               (onExternalViewChange [views _]
                                 (dorun (pmap #(viewChangeListener-handleView % connectString clusterName) views))))]
    (doto manager
      (.connect)
      (.addLiveInstanceChangeListener liveInstanceChangeListener)
      (.addExternalViewChangeListener externalViewListener))))



(def singleHelixAdministrator (memoize createHelixAdministrator))

(defn getClusters [^String connectString]
  (let [client (singleCuratorFramework connectString)]
    (getChildren client (getCurrentClustersKey))))

