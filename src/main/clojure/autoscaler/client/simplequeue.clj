(ns autoscaler.client.simplequeue
  (:import (org.apache.curator.framework.recipes.queue SimpleDistributedQueue)
           (org.apache.curator.framework CuratorFramework))
  (:use [autoscaler.log]
        [autoscaler.utils :as utils])
  )

(defprotocol SimpleQueue
  (soffer [this value])
  (stake [this])
  (speek [this])
  (sremove [this]))

(defn- simpleQueue->offer [^SimpleDistributedQueue queue ^String path ^String value]
  (try
    (if (not (.offer queue (.getBytes value)))
      (do (utils/sleep 1000)
          (simpleQueue->offer queue path value))
      (log-debug (str "offer value " value " for path " path)))
    (catch Exception e
      (log-warn-error e (str "fail to offer the data " value " for path " path))
      (utils/sleep 1000)
      (simpleQueue->offer queue path value))))

(defn- ^String simpleQueue->take [^SimpleDistributedQueue queue ^String path]
  (try
    (String. (.take queue))
    (catch Exception e
      (log-warn-error e (str "fail to take the data for " path))
      nil)))

(defn- ^String simpleQueue->peek [^SimpleDistributedQueue queue ^String path]
  (try
    (String. (.peek queue))
    (catch Exception e
      (log-warn-error e (str "fail to peek the data for " path))
      nil)))

(defn- ^String simpleQueue->remove [^SimpleDistributedQueue queue ^String path]
  (try
    (String. (.remove queue))
    (catch Exception e
      (log-warn-error e (str "fail to remove the data for " path))
      nil)))

(defn ^SimpleQueue createSimpleQueue [^CuratorFramework client ^String path]
  (let [queue (SimpleDistributedQueue. client path)]
    (log-message (str "create simple queue for path " path))
    (reify
      SimpleQueue
      (soffer [_ value]
        (simpleQueue->offer queue path value))
      (stake [_]
        (simpleQueue->take queue path))
      (speek [_]
        (simpleQueue->peek queue path))
      (sremove [_]
        (simpleQueue->remove queue path)))))


(def singleSimpleQueue (memoize createSimpleQueue))