(ns autoscaler.client.mondataqueue
  (:import (java.util Date)
           (org.apache.curator.framework.recipes.queue QueueSerializer QueueConsumer DistributedQueue QueueBuilder)
           (org.apache.curator.framework CuratorFramework))
  (:use [clojure.string :only [split]])
  (:use [autoscaler.log]
        [autoscaler.utils :as utils])
  )

(defprotocol DataToString
  (objToString [this]))

(defrecord MonData [^double cpuUsage ^double memoryUsage ^Date timestamp ^String clusterName]
  DataToString
  (objToString [_]
    (str "cpuUsage=" cpuUsage
         ",memoryUsage=" memoryUsage
         ",timestamp=" (.getTime timestamp)
         ",clusterName=" clusterName)))

(defn ^MonData parse [^String value]
  (let [values (reduce #(assoc %1 (keyword (first %2)) (fnext %2)) {} (map #(split % #"=") (split value #",")))
        parseDouble (fn [^String value] (Double/parseDouble value))
        parseDate (fn [^String value] (Date. (Long/parseLong value)))]
    (MonData.
      (parseDouble (:cpuUsage values))
      (parseDouble (:memoryUsage values))
      (parseDate (:timestamp values))
      (:clusterName values))))

(defn- createMonDataQueueSerializer []
  (reify
    QueueSerializer
    (serialize [_ data]
      (.getBytes (objToString data)))
    (deserialize [_ value]
      (parse (String. value)))))

(defprotocol DataQueueConsumer
  (cleanUpConsumerData [this])
  (getConsumerData [this]))

(defn- createDataQueueConsumer []
  (let [dataQueueConsumerValueWrapper (ref '())]
    (reify
      QueueConsumer
      (consumeMessage [_ var1]
        (dosync (alter dataQueueConsumerValueWrapper conj var1)))
      (stateChanged [_ _ _])
      DataQueueConsumer
      (cleanUpConsumerData [_]
        (dosync (alter dataQueueConsumerValueWrapper empty)))
      (getConsumerData [_]
        (deref dataQueueConsumerValueWrapper)))))

(defprotocol DataQueue
  (cleanUp [this])
  (getData [this])
  (putData [this value]))


(defn ^DataQueue createMonDataQueue [^CuratorFramework client ^String path]
  (let [consumer (createDataQueueConsumer)
        queue (let [queue (.buildQueue (QueueBuilder/builder client consumer (createMonDataQueueSerializer) path))]
                (.start queue)
                (.addShutdownHook (Runtime/getRuntime) (proxy [Thread] []
                                                         (run []
                                                           (.close queue))))
                queue)]
    (reify DataQueue
      (cleanUp [_]
        (cleanUpConsumerData consumer))
      (getData [_]
        (getConsumerData consumer))
      (putData [_ value]
        (try
          (.put queue value)
          (catch Exception e
            (log-error e (str "fail to add data to the data queue for path " path)))))
      )
    ))

(def singleMonDataQueue (memoize createMonDataQueue))






