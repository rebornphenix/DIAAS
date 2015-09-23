(ns autoscaler.core
  (:gen-class)
  (:import (org.apache.curator.test TestingServer))
  (:use [autoscaler.log]
        [autoscaler.cluster.quickstart]))

(defn doAction [action args]
  (try
    ((resolve (symbol (str "autoscaler.core/" action))) args)
    (catch IllegalArgumentException e (log-message "Please input the correct action (clusterManager|clusterAgent)")))
  )

(defn quickStartTest [args]
  (let [connnectString (.getConnectString (TestingServer.))]
    (log-message )
    (quickStart connnectString)))

(defn -main
  "the entry point for whole application"
  [& args]
  (do (log-message "Starting......")
      (let [[action & actionArgs] args]
        (doAction action actionArgs))))
