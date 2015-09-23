(ns autoscaler.client.client
  (:import (org.apache.curator.framework CuratorFramework CuratorFrameworkFactory)
           (org.apache.curator.retry ExponentialBackoffRetry)
           (org.apache.zookeeper CreateMode)
           (org.apache.curator.framework.imps CuratorFrameworkState))
  (:use [autoscaler.log]
        [autoscaler.utils :as utils])
  )

(defn- ensureStarted [^CuratorFramework client]
  (if (not (= (.getState client) (CuratorFrameworkState/STARTED)))
    (.start client)))

(defn ^CuratorFramework createCuratorFramework [^String connectString]
  (let [client (CuratorFrameworkFactory/newClient connectString (ExponentialBackoffRetry. 1000 3))]
    (do
      (ensureStarted client)
      client)))

(def singleCuratorFramework (memoize createCuratorFramework))

(defprotocol DataUsage
  (addChild [this path value])
  (delete [this path])
  (exist [this path])
  (getChildren [this path])
  (setData [this path data])
  (getData [this path])
  )

(extend-type CuratorFramework
  DataUsage
  (addChild [this path value]
    (try
      (ensureStarted this)
      (.forPath (.withMode (.creatingParentsIfNeeded (.create this)) (CreateMode/PERSISTENT)) (str path "/" value))
      (catch Exception e
        (log-error e (str "fail to add child under path " path)))))
  (exist [this path]
    (try
      (ensureStarted this)
      (let [stat (.forPath (.watched (.checkExists this)) path)]
        (not (nil? stat)))
      (catch Exception e
        (log-error e (str "fail to check the exist of path " path))
        false)))
  (delete [this path]
    (ensureStarted this)
    (if (exist this path)
      (try
        (.forPath (.delete this) path)
        (catch Exception e
          (log-error e (str "fail to delete the path " path))))))
  (getChildren [this path]
    (try
      (ensureStarted this)
      (.forPath (.getChildren this) path)
      (catch Exception e
        (log-error e (str "fail to get the children of the path " path))
        '())))
  (setData [this path data]
    (ensureStarted this)
    (delete this path)
    (try
      (.forPath (.withMode (.creatingParentsIfNeeded (.create this)) (CreateMode/PERSISTENT)) path (.getBytes data))
      (catch Exception e
        (log-error e (str "fail to set the data " data " to the path of " path)))))
  (getData [this path]
    (try
      (ensureStarted this)
      (String. (.forPath (.getData this) path))
      (catch Exception e
        (log-error e (str "fail to get the data from the path " path))
        "")))
  )


