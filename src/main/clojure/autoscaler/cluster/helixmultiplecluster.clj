(ns autoscaler.cluster.helixmultiplecluster
  (:import (org.apache.helix.manager.zk ZKHelixAdmin ZKHelixManager)
           (org.apache.helix.model BuiltInStateModelDefinitions InstanceConfig)
           (org.apache.helix InstanceType)
           (org.apache.helix.participant DistClusterControllerStateModelFactory)
           (org.apache.helix.tools ClusterStateVerifier ClusterStateVerifier$BestPossAndExtViewZkVerifier))
  (:use [autoscaler.keys]
        [autoscaler.cluster.zkclientpool :as pool]
        [autoscaler.cluster.helix]
        [autoscaler.cluster.model :as model]
        [autoscaler.utils :as utils]
        [autoscaler.log]))

(defn- addExistingModels [^ZKHelixAdmin admin ^String clusterName]
  (map #(.addStateModelDef admin clusterName (.getId (.getStateModelDefinition %)) (.getStateModelDefinition %)) (BuiltInStateModelDefinitions/values)))

(defn createGrandCluster [^String connectString]
  (let [client (pool/zkClient connectString)
        admin (ZKHelixAdmin. client)]
    (ensureClusterNotExist client DEFAULT_GRAND_CLUSTER_NAME)
    (.addCluster admin DEFAULT_GRAND_CLUSTER_NAME true)
    (addExistingModels admin DEFAULT_GRAND_CLUSTER_NAME))
  )

(defn- createGrandController [^String connectString ^InstanceConfig instance]
  (let [client (ZKHelixAdmin. (pool/zkClient connectString))
        instanceId (.getId instance)
        manager (ZKHelixManager. DEFAULT_GRAND_CLUSTER_NAME instanceId (InstanceType/CONTROLLER_PARTICIPANT) connectString)]
    (.addInstance client DEFAULT_GRAND_CLUSTER_NAME instance)
    (.registerStateModelFactory (.getStateMachineEngine manager) DEFAULT_GRAND_CLUSTER_CONTROLLER_AGENT_MODEL (DistClusterControllerStateModelFactory. connectString))
    (.connect manager)
    (.addShutdownHook (Runtime/getRuntime) (proxy [Thread] []
                                             (run []
                                               (.disconnect manager))))
    (log-message (str "instance " instanceId " has been successfully started!"))))

(defn- createGrandControllerWithOnlyHost [^String connectString ^String host]
  (createGrandController connectString (createInstanceConfig host DEFAULT_CONTROLLER_PORT)))

(def singleHelixController (memoize createGrandControllerWithOnlyHost))

(defprotocol HelixAutoscalerManager
  (init [this clusterName])
  (rebalance [this clusterName rebalanceReplica]))

(defn- ^HelixAutoscalerManager createHelixManager [^String connectString ^String resourceName]
  (let [client (pool/zkClient connectString)
        admin (ZKHelixAdmin. client)
        stateModelRef DEFAULT_HELIX_STATE_MODEL_REF
        partitions DEFAULT_HELIX_CLUSTER_PARTITION
        grandClusterName DEFAULT_GRAND_CLUSTER_NAME]
    (reify
      HelixAutoscalerManager
      (init [_ clusterName]
        (ensureClusterNotExist client clusterName)
        (.addCluster admin clusterName true)
        (when-not (ClusterStateVerifier/verifyByPolling (ClusterStateVerifier$BestPossAndExtViewZkVerifier. connectString grandClusterName))
          (utils/sleep 1000))
        (.addClusterToGrandCluster admin clusterName grandClusterName)
        (.addStateModelDef admin clusterName stateModelRef (model/defineStateModel stateModelRef))
        (.addResource admin clusterName resourceName partitions stateModelRef))
      (rebalance [_ clusterName rebalanceReplica]
        (do
          (let [currentSize (getClusterCurrentSize connectString clusterName)]
            (if (< rebalanceReplica currentSize)
              (do
                (.rebalance admin clusterName resourceName rebalanceReplica)
                (sleep 10000)))
            (setClusterIdealSize connectString clusterName rebalanceReplica)))))))

(def singleHelixManager (memoize createHelixManager))


