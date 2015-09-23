(ns autoscaler.client.lock-test
  (:require [clojure.test :refer :all]
            [autoscaler.client.client :refer :all]
            [autoscaler.client.lock :refer :all]
            )
  (:import (org.apache.curator.test TestingServer)))

(defrecord WrapperValue [values millis]
  Command
  (execute [this]
    (do (try (Thread/sleep millis)
             (catch Exception e (.printStackTrace e))) (dosync (alter values conj (System/currentTimeMillis))))))

(deftest try-lock-test
         (testing "Running lock"
           (let [connectString (.getConnectString (TestingServer.))
                 client (singleCuratorFramework connectString)
                 path "/testlockpath"
                 lock (singleLock client path)
                 valueHolder (ref ())
                 wrapperValue (WrapperValue. valueHolder 1000)]
             (pcalls (run lock wrapperValue)
                     (run lock wrapperValue))
                 (is (> (- (first (deref valueHolder)) (fnext (deref valueHolder))) 900)))))
