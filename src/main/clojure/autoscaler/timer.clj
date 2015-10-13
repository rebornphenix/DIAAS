(ns autoscaler.timer
  (:import (java.util.concurrent Executors TimeUnit)))

(defn onceRun [countOfThread ^Runnable command]
  (.execute (Executors/newFixedThreadPool countOfThread) command))

(defn onceDefaultRun [^Runnable command]
  (onceRun 1 command))

(defn repeatedRun [countOfThread ^Runnable command initialDelay period ^TimeUnit unit]
  (.scheduleAtFixedRate (Executors/newScheduledThreadPool countOfThread)
                       command initialDelay period unit))

(defn repeatedDefaultRun [^Runnable command]
  (repeatedRun 1 command 10 10 (TimeUnit/SECONDS)))

