(ns autoscaler.cluster.zkclientpool
  (:import (org.apache.helix.manager.zk ZkClient ZNRecordSerializer ZKHelixAdmin)
           (org.apache.zookeeper ZooKeeper$States)
           (java.util.concurrent TimeUnit))
  (:use [autoscaler.log]))

(defprotocol ZKClientPool
  (^ZkClient getZkClient [this ^String connectString]))

(defn- ensureLiveInstances [pool]
  (into {} (filter #(= (.getZookeeperState (.getConnection (fnext %1))) (ZooKeeper$States/CONNECTED)) pool)))

(defn- ^ZKClientPool createZkClientPool []
  (let [pool (ref {})]
    (reify
      ZKClientPool
      (getZkClient [this connectString]
        (dosync (alter pool ensureLiveInstances))
        (if (not (contains? (deref pool) connectString))
          (let [client (ZkClient. connectString (ZkClient/DEFAULT_SESSION_TIMEOUT) (ZkClient/DEFAULT_CONNECTION_TIMEOUT) (ZNRecordSerializer.))
                timeOutInSec (Integer/valueOf (System/getProperty (ZKHelixAdmin/CONNECTION_TIMEOUT) "30"))
                reconnect (fn [] (getZkClient this connectString))]
            (if (not (try  (.waitUntilConnected client timeOutInSec (TimeUnit/SECONDS))
                           (catch Exception e
                             (log-error e (str "fail to set up the connnection to " connectString))
                             (reconnect))))
              (reconnect))
            (dosync (alter pool assoc connectString client))
            ))
        (get (deref pool) connectString)))))

(def ^:private zkClientPool (memoize createZkClientPool))

(defn zkClient[^String connectString]
  (getZkClient (zkClientPool) connectString))