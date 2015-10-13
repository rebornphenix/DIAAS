(ns autoscaler.scheduler
  (:require
    [clojure.set :refer [difference]]
    [autoscaler.timer :as timer]
    [autoscaler.utils :refer [sleep]])
  (:use
    [autoscaler.keys]
    [autoscaler.client.lock]
    [autoscaler.client.client]
    [autoscaler.log]
    [autoscaler.cluster.status]
    [autoscaler.cluster.helix]
    [autoscaler.client.simplequeue]
    ))

(defn- randomIp []
  (str "192.168.1." (format "%d" (rand-int 10000))))

(defn- waitForStatus [^String connectString ^String agentIp]
  (let [client (singleCuratorFramework connectString)
        queue (singleSimpleQueue client (getHelixAgentDoneStatusKey agentIp))]
    (do
      (stake queue)
      (sleep 10000))))

(defn- newAgentsForCluster [^String connectString ^String clusterName ^long size]
  (let [ips (repeatedly size randomIp)]
    (do
      (dorun (map #(log-message (str "=============" %)) ips))
      (dorun
        (map
          #(singleHelixAgent (singleCuratorFramework connectString) connectString clusterName %)
          ips))
      (dorun
        (map #(waitForStatus connectString %) ips)))))

(defn- scaleOutCluster [^String connectString ^String clusterName]
  (let [lock (createClusterLockFor connectString clusterName getClusterScaleOutLockKey)]
    (run lock
         (reify Command
           (execute [_]
             (let [idealSize (getClusterIdealSize connectString clusterName)
                   currentSize (getClusterCurrentSize connectString clusterName)]
               (if (> idealSize currentSize)
                 (newAgentsForCluster connectString clusterName (- idealSize currentSize)))))))))

(defn- handleScaleOut [^String connectString ^String clusterName]
  (timer/onceDefaultRun (proxy [Runnable] []
                          (run []
                            (scaleOutCluster connectString clusterName)))))

(defn- deleteAgentFromCluster [^String connectString ^String clusterName ^String hostIp]
  (log-message (str "send single to delete host " hostIp))
  (deleteToBeRemovedFromCluster connectString clusterName hostIp))

(defn- handleScaleIn [^String connectString ^String clusterName]
  (dorun
    (map #(deleteAgentFromCluster connectString clusterName %) (getToBeRemovedsFromCluster connectString clusterName))))

(defn- populateScaleOutClusters [^String connectString]
  (dorun (map #(do
                (handleScaleOut connectString %)
                (handleScaleIn connectString %))
              (getClusters connectString))))


(defn startScheduler [^String connectString]
  (timer/repeatedDefaultRun (proxy [Runnable] []
                              (run []
                                (populateScaleOutClusters connectString)))))



