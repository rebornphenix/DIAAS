(ns autoscaler.cluster.model
  (:import (org.apache.curator.framework CuratorFramework)
           (org.apache.helix.model StateModelDefinition$Builder StateModelDefinition)
           (autoscaler.cluster MasterSlaveStateModel MasterSlaveStateModelFactory MasterSlaveStateModelFactoryImpl)
           (org.apache.helix.participant.statemachine StateModelFactory))
  (:use [autoscaler.keys]
        [autoscaler.client.client]
        [autoscaler.client.simplequeue]
        [autoscaler.status]
        [autoscaler.log]
        [autoscaler.utils :as utils]))

(defprotocol MasterAgentStatus
  (setCurrentMasterIp [this clusterName masterIp])
  (getCurrentMasterIp [this clusterName])
  (deleteMasterIp [this clusterName])
  )

(defn- masterAgentStatus->getCurrentMasterIp [^CuratorFramework client ^String clusterName]
  (try
    (let [masterIp (getData client (getCurrentMasterIpKey clusterName))]
      (if (.equals masterIp "")
        (do
          (sleep 100)
          (masterAgentStatus->getCurrentMasterIp client clusterName))
        masterIp))
    (catch Exception e
      (log-warn-error e (str "fail to get master ip for cluster " clusterName))
      (utils/sleep 1000)
      (masterAgentStatus->getCurrentMasterIp client clusterName)
      )))

(defn- ^MasterAgentStatus createMasterAgentStatus [^CuratorFramework client]
  (reify
    MasterAgentStatus
    (setCurrentMasterIp [_ clusterName masterIp]
      (setData client (getCurrentMasterIpKey clusterName) masterIp))
    (getCurrentMasterIp [_ clusterName]
      (masterAgentStatus->getCurrentMasterIp client clusterName))
    (deleteMasterIp [_ clusterName]
      (delete client (getCurrentMasterIpKey clusterName)))))


(defn- printTransitionMessage [resourceName partitionName instanceName message]
  (log-message (str instanceName " transitioning from " (.getFromState message) " to " (.getToState message) " for resource " resourceName " and partition " partitionName)))



(defn- createExtendedMasterSlaveModel [^String resourceName ^String partitionName ^String instanceName ^String hostIp transDelay masterAgentStatus]
  (reify MasterSlaveStateModel
    (onBecomeMasterFromOffline [_ message context]
      (let [clusterName (.getClusterName (.getManager context))]
        (deleteMasterIp masterAgentStatus clusterName)
        (utils/executeCmd "/usr/local/bin/helix_from_offline_to_master.sh" hostIp resourceName)
        (setCurrentMasterIp masterAgentStatus clusterName hostIp)
        (utils/sleep transDelay)
        (printTransitionMessage resourceName partitionName instanceName message)
        ))
    (onBecomeSlaveFromOffline [_ message context]
      (let [clusterName (.getClusterName (.getManager context))
            masterIp (getCurrentMasterIp masterAgentStatus clusterName)]
        (utils/executeCmd "/usr/local/bin/helix_from_offline_to_slave.sh" masterIp hostIp resourceName)
        (utils/sleep transDelay)
        (printTransitionMessage resourceName partitionName instanceName message)
        ))
    (onBecomeMasterFromSlave [_ message context]
      (let [clusterName (.getClusterName (.getManager context))]
        (utils/executeCmd "/usr/local/bin/helix_from_slave_to_master.sh" hostIp resourceName)
        (utils/sleep transDelay)
        (setCurrentMasterIp masterAgentStatus clusterName hostIp)
        (printTransitionMessage resourceName partitionName instanceName message)
        ))
    (onBecomeSlaveFromMaster [_ message _]
      (utils/executeCmd "/usr/local/bin/helix_from_master_to_slave.sh" hostIp resourceName)
      (utils/sleep transDelay)
      (printTransitionMessage resourceName partitionName instanceName message)
      )
    (onBecomeOfflineFromMaster [_ message context]
      (let [clusterName (.getClusterName (.getManager context))]
        (deleteMasterIp masterAgentStatus clusterName)
        (utils/executeCmd "/usr/local/bin/helix_from_master_to_offline.sh" hostIp resourceName)
        (utils/sleep transDelay)
        (printTransitionMessage resourceName partitionName instanceName message)
        ))
    (onBecomeOfflineFromSlave [_ message context]
      (let [clusterName (.getClusterName (.getManager context))
            masterIp (getCurrentMasterIp masterAgentStatus clusterName)]
        (utils/executeCmd "/usr/local/bin/helix_from_slave_to_offline.sh" masterIp hostIp resourceName)
        (utils/sleep transDelay)
        (printTransitionMessage resourceName partitionName instanceName message)
        ))
    (onBecomeDroppedFromOffline [_ message _]
      (utils/executeCmd "/usr/local/bin/helix_from_offline_to_dropped.sh" hostIp resourceName)
      (utils/sleep transDelay)
      (printTransitionMessage resourceName partitionName instanceName message))
    ))

(defn ^StateModelFactory createStateModelFactory [^CuratorFramework client ^String instanceName ^String hostIp]
  (MasterSlaveStateModelFactoryImpl. (reify
                                       MasterSlaveStateModelFactory
                                       (^MasterSlaveStateModel createNewStateModel [_ ^String resourceName ^String partitionName]
                                         (createExtendedMasterSlaveModel resourceName partitionName instanceName hostIp (long (* 1000 60 1)) (createMasterAgentStatus client))))))

(defn ^StateModelDefinition defineStateModel [^String stateModel]
  (let [builder (doto (StateModelDefinition$Builder. stateModel)
                  (.addState AGENT_STATUS_MASTER 1)
                  (.addState AGENT_STATUS_SLAVE 2)
                  (.addState STATUS_OFFLINE)
                  (.addState AGENT_STATUS_DROPPED)
                  (.initialState STATUS_OFFLINE)
                  (.addTransition STATUS_OFFLINE AGENT_STATUS_MASTER 100)
                  (.addTransition STATUS_OFFLINE AGENT_STATUS_SLAVE 99)
                  (.addTransition AGENT_STATUS_SLAVE AGENT_STATUS_MASTER 99)
                  (.addTransition AGENT_STATUS_MASTER AGENT_STATUS_SLAVE 98)
                  (.addTransition AGENT_STATUS_MASTER STATUS_OFFLINE 1)
                  (.addTransition AGENT_STATUS_SLAVE STATUS_OFFLINE 1)
                  (.addTransition STATUS_OFFLINE AGENT_STATUS_DROPPED 0)
                  (.upperBound AGENT_STATUS_MASTER 1)
                  (.dynamicUpperBound AGENT_STATUS_SLAVE "R"))]
    (.build builder)))





