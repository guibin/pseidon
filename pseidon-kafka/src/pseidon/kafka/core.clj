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
  (defn start-logging []
  (org.apache.log4j.BasicConfigurator/configure))
  
  (start-logging)
  
  (use 'pseidon.kafka.util)
  (use 'pseidon.core.conf)
  
  (def base-config {
                    :kafka.zk.connect "localhost:2181"; "192.168.56.101"
                    :kafka.groupid "test--group"
                    :kafka.zk.connectiontimeout.ms "1000"
                    
                    
                    })
  
  (doseq [[k v] base-config] (set-conf! k v))
  (def kconf (get-kafka-conf))
  
  (def ds (load-datasink kconf))
  (def writer (:writer ds))
  (writer [{:topic "test1" :val [ (.getBytes "hi")] }])
  
  (def ds-source (load-datasource kconf))
  (def messages ((:reader-seq ds-source) "abc"))
  (take 10 messages)
  (+ 1 2)
  )