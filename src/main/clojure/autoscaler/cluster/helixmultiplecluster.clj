(ns autoscaler.cluster.helixmultiplecluster
  (:import (org.apache.helix.manager.zk ZKHelixAdmin ZKHelixManager)
           (org.apache.helix.model BuiltInStateModelDefinitions InstanceConfig IdealState$RebalanceMode)
           (org.apache.helix InstanceType)
           (org.apache.helix.participant DistClusterControllerStateModelFactory)
           (org.apache.helix.tools ClusterStateVerifier ClusterStateVerifier$BestPossAndExtViewZkVerifier))
  (:use [autoscaler.keys]
        [autoscaler.cluster.zkclientpool :as pool]
        [autoscaler.cluster.helix]
        [autoscaler.cluster.model :as model]
        [autoscaler.utils :as utils]))

(defn- addExistingModels[^ZKHelixAdmin admin ^String clusterName]
  (map #(.addStateModelDef admin clusterName (.getId (.getStateModelDefinition %)) (.getStateModelDefinition %)) (BuiltInStateModelDefinitions/values)))

(defn createGrandCluster [^String connectString]
  (let [client (pool/zkClient connectString)
        admin (ZKHelixAdmin. client)]
    (ensureClusterNotExist client DEFAULT_GRAND_CLUSTER_NAME)
    (.addCluster admin DEFAULT_GRAND_CLUSTER_NAME true)
    (addExistingModels admin DEFAULT_GRAND_CLUSTER_NAME))
    )

(defn createGrandController [^String connectString ^InstanceConfig instance]
  (let [client (ZKHelixAdmin. (pool/zkClient connectString))
        manager (ZKHelixManager. DEFAULT_GRAND_CLUSTER_NAME (.getId instance) (InstanceType/CONTROLLER_PARTICIPANT) connectString)]
    (.addInstance client DEFAULT_GRAND_CLUSTER_NAME instance)
    (.registerStateModelFactory (.getStateMachineEngine manager) DEFAULT_GRAND_CLUSTER_CONTROLLER_AGENT_MODEL (DistClusterControllerStateModelFactory. connectString))
    (.connect manager)
    (.addShutdownHook (Runtime/getRuntime) (proxy [Thread] []
                                             (run[]
                                               (.disconnect manager))))
    ))


(defprotocol HelixAutoscalerManager
  (init [this clusterName])
  (rebalance [this clusterName rebalanceReplica]))

(defn ^HelixAutoscalerManager createHelixManager [^String connectString]
  (let [client (pool/zkClient connectString)
        admin (ZKHelixAdmin. client)
        resourceName DEFAULT_HELIX_RESOURCE_NAME
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
        (.addResource admin clusterName resourceName partitions stateModelRef (.toString IdealState$RebalanceMode/FULL_AUTO)))
      (rebalance [_ clusterName rebalanceReplica]
        (.rebalance admin clusterName resourceName rebalanceReplica)))))

(def singleHelixManager (memoize createHelixManager))