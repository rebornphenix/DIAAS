(ns autoscaler.client.lock
  (:import (org.apache.curator.framework.recipes.locks InterProcessMutex)
           (org.apache.curator.framework CuratorFramework))
  (:use [autoscaler.log]
        [autoscaler.utils :as utils])
  )

(defprotocol Command
  (execute [this]))

(defprotocol RunInLock
  (run [this command]))

(defn- ^RunInLock createLock [^CuratorFramework client ^String path]
  (let [lock (InterProcessMutex. client path)]
    (reify RunInLock
      (run [this command]
        (try
          (.acquire lock)
          (log-message (str "accquire the lock for path " path))
          (catch Exception e
            (log-error e (str "Error to get lock for path " path))
            (utils/sleep 1000)
            (run this command)))
        (try (execute command)
             (catch Exception e
               (log-error e (str "Error to run within the lock for path " path)))
             (finally (try (do (.release lock) (log-message (str "release the lock for path" path)))
                           (catch Exception e (log-error e (str "Error to release the lock for path " path))))))
        ))))

(def singleLock (memoize createLock))

