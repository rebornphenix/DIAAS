(ns autoscaler.core
  (:gen-class)
  (:import (org.apache.curator.test TestingServer))
  (:use [autoscaler.log]
        [autoscaler.cluster.quickstartsinglecluster :as single]
        [autoscaler.cluster.quickstartmultiplecluster :as multiple]
        [autoscaler.cluster.quickstartscaleout :as scaleout]))

(defn doAction [action args]
  (try
    ((resolve (symbol (str "autoscaler.core/" action))) args)
    (catch IllegalArgumentException e (log-error e "Please input the correct action (clusterManager|clusterAgent)")))
  )

(defn quickStartSingleClusterTest [args]
  (let [connnectString (.getConnectString (TestingServer.))]
    (log-message (str "start server on " connnectString))
    (single/quickStartSingle connnectString)))

(defn quickStartMultipleClusterTest [args]
  (let [connnectString (.getConnectString (TestingServer.))]
    (log-message (str "start server on " connnectString))
    (multiple/quickStartMultiple connnectString)))

(defn quickStartScaleOutTest [args]
  (let [connnectString (.getConnectString (TestingServer.))]
    (log-message (str "start server on " connnectString))
    (scaleout/quickStartScaleOut connnectString)))

(defn -main
  "the entry point for whole application"
  [& args]
  (do (log-message "Starting......")
      (let [[action & actionArgs] args]
        (doAction action actionArgs))))
