(ns autoscaler.cluster.status
  (:use [autoscaler.client.lock :as lock]
        [autoscaler.keys]
        [autoscaler.client.client :as client]
        [autoscaler.log]))

(defn createClusterLockFor [^String connectString ^String clusterName keyFunc]
  (let [client (client/singleCuratorFramework connectString)]
    (lock/singleLock client (keyFunc clusterName))))

(defn addToBeRemovedToCluster [^String connectString ^String clusterName ^String hostIp]
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

(defn getClusterStatus [^String connectString ^String clusterName defaultValueFunc lockKeyFunc nodeCacheFunc]
  (let [client (client/singleCuratorFramework connectString)
        lock (createClusterLockFor connectString clusterName lockKeyFunc)
        valuePath (nodeCacheFunc clusterName)
        retValue (ref '())]
    (run lock (reify Command
                (execute [_]
                  (let [value (getDataWithDefaultValue client valuePath (defaultValueFunc))]
                    (dosync (alter retValue conj value))))))
    (first (deref retValue))))

(defn- setClusterStatus [^String connectString ^String clusterName status lockKeyFunc nodeCacheFunc logFunc]
  (let [client (client/singleCuratorFramework connectString)
        lock (createClusterLockFor connectString clusterName lockKeyFunc)
        valuePath (nodeCacheFunc clusterName)]
    (run lock (reify Command
                (execute [_]
                  (setData client valuePath status)
                  (logFunc))))))

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

(defn getClusters [^String connectString]
  (let [client (singleCuratorFramework connectString)]
    (getChildren client (getCurrentClustersKey))))





