(ns autoscaler.client.nodecache
  (:import (org.apache.curator.framework CuratorFramework)
           (org.apache.curator.framework.recipes.cache NodeCacheListener))
  (:use [autoscaler.log]
        [autoscaler.utils :as utils])
  )

(defprotocol NodeCache
  (setValue [this value])
  (getValue [this]))

(defprotocol NodeCacheListenerDataRetriever
  (getData [this]))

(defn- createNodeCacheListener [^org.apache.curator.framework.recipes.cache.NodeCache nodecache ^String path]
  (let [nodeCacheListenerWrapper (ref '())]
    (reify
      NodeCacheListener
      (nodeChanged [_]
        (let [currentData (String. (.getData (.getCurrentData nodecache)))]
          (dosync (alter nodeCacheListenerWrapper conj currentData))
          (log-debug (str "receive the data " currentData " for path " path))))
      NodeCacheListenerDataRetriever
      (getData [this]
        (let [ret (first (deref nodeCacheListenerWrapper))]
          (if (nil? ret)
            (do
              (sleep 100)
              (getData this)))
          (dosync (alter nodeCacheListenerWrapper empty))
          ret)))))

(defn- ensurePathCreated [^CuratorFramework client ^String path ^String defaultValue]
  (if (nil? (.forPath (.checkExists client) path))
    (do (.forPath (.creatingParentsIfNeeded (.create client)) path (.getBytes defaultValue)))))

(defn- ^NodeCache createNodeCache [^CuratorFramework client ^String path ^String defaultValue]
  (let [cache (org.apache.curator.framework.recipes.cache.NodeCache. client path)
        listener (createNodeCacheListener cache path)]
    (.addListener (.getListenable cache) listener)
    (.start cache)
    (ensurePathCreated client path defaultValue)
    (utils/sleep 1000)
    (log-message (str "create node cache for " path))
    (reify
      NodeCache
      (setValue [_ value]
        (.forPath (.setData client) path (.getBytes value))
        (log-debug (str "set data " value " for path " path)))
      (getValue [_]
        (let [value (String. (.getData listener))]
          (log-debug (str "get data " value " for path " path))
          value)))))

(def singleNodeCache (memoize createNodeCache))


