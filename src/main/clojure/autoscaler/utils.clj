(ns autoscaler.utils
  (:use [autoscaler.log])
  (:import (org.apache.commons.exec CommandLine DefaultExecuteResultHandler ExecuteWatchdog DefaultExecutor)))

(defn sleep [^long millis]
  (try
    (Thread/sleep millis)
    (catch InterruptedException e (log-message e))))

(defn executeCmd [^String executable & args]
  (try
    (let [cmdLine (CommandLine. executable)
          resultHandler (DefaultExecuteResultHandler.)
          watchdog (ExecuteWatchdog. (Long/valueOf -1))]
      (map #(.addArgument cmdLine %) args)
      (doto (DefaultExecutor.)
        (.setExitValue 1)
        (.setWatchdog watchdog)
        (.execute cmdLine resultHandler))
      (.waitFor resultHandler)
      (if (not (= (.getExitValue resultHandler) 0))
        (throw (Exception. (str "Command " executable " is failed"))))
      )
    (catch Exception e
      (log-error e (str "fail to execute command " executable args))
      (sleep 1000)
      (apply executeCmd executable args))))
