(ns autoscaler.timer
  (:import (java.util.concurrent Executors TimeUnit)))

(defn schedule [^int countOfThread ^Runnable command ^long initialDelay ^long period ^TimeUnit unit]
  (scheduleAtFixedRate (Executors/newFixedThreadPool countOfThread)
                       command initialDelay period unit))

(defn scheduleWithDefaultSetting [^Runnable command]
  (schedule 1 command 10 1 (TimeUnit/SECONDS)))

