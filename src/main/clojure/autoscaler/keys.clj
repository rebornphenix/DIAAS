(ns autoscaler.keys)

(defn getCurrentMasterIpKey [clusterName] (str "/auto_scaling/current_clusters/" clusterName "/master_ip"))

(defn getHelixAgentDoneStatusKey [agentIp] (str "/auto_scaling/helix_agent/done_status/" agentIp))

(defn getCurrentClustersKey [] "/auto_scaling/current_clusters")

(defn getClusterScaleOutLockKey [clusterName] (str "/auto_scaling/current_clusters/" clusterName "/lock/scale_out"))

(defn getClusterToBeRemovedKey [clusterName] (str "/auto_scaling/current_clusters/" clusterName "/to_be_removed"))

(defn getClusterIdealSizeKey [clusterName] (str "/auto_scaling/current_clusters/" clusterName "/ideal_size"))

(defn getClusterStatusLockKey [clusterName] (str "/auto_scaling/current_clusters/" clusterName "/lock/status"))

(defn getClusterCurrentSizeKey [clusterName] (str "/auto_scaling/current_clusters/" clusterName "/current_size"))

(def DEFAULT_HELIX_CLUSTER_PARTITION 1)

(def DEFAULT_HELIX_RESOURCE_NAME "UniqueResource")

(def DEFAULT_HELIX_STATE_MODEL_REF "ExtendedMasterSlave")

(def DEFAULT_AGENT_PORT (long 12000))

(def DEFAULT_CONTROLLER_PORT (long 13000))

(def DEFAULT_SPECTATOR_PORT (long 14000))

(def DEFAULT_TEST_HELIX_CLUSTER_NAME "DEFAULT_TEST_HELIX_CLUSTER_NAME")

(def DEFAULT_GRAND_CLUSTER_NAME "GRAND_CLUSTER_FOR_CONTROLLER")

(def DEFAULT_GRAND_CLUSTER_CONTROLLER_AGENT_MODEL "LeaderStandby")