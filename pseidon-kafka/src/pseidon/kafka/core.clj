(ns pseidon.kafka.core
  (:gen-class)
  (:require [clojure.tools.cli :refer [cli]]
            )
  (:use pseidon.kafka.util)
  (:import (reply ReplyMain)
           (java.io File))
  )


(defn cmd [args] 
  (cli args
    ["-repl" "--repl" "Start a repl instance" :flag true]
  ))



(defn run-repl [opts]
  (reply.ReplyMain/main nil))

(defn -main [& args]
  (if-let [[opts _ usage] (cmd args)]
    (do
       (cond (:repl opts) (run-repl opts))
       )
    (prn (cmd args))))


(comment 
  (use 'pseidon.kafka.util)
  (use 'pseidon.core.conf)
  
  (def base-config {
                    :kafka.zookeeper.connect "10.101.4.142:2181"
                    :kafka.groupid "test"
                    
                    
                    })
  
  (doseq [[k v] base-config] (set-conf! k v))
  (def kconf (get-kafka-conf))
  
  (def ds (load-datasink kconf))
  (def writer (:writer ds))
  (writer [["test1" (.getBytes "hi")]])
  )