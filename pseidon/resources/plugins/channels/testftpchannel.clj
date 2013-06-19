(ns plugins.channels.testftpchannel
  
  (:require 
      [pseidon.core.datastore :refer [get-data-long]]
      [pseidon.core.conf :refer [get-conf2]]
      [pseidon.core.queue :refer [publish]]
      [pseidon.core.app :refer [data-queue]]
      [pseidon.core.conf :refer [set-conf!]]
      [pseidon.core.watchdog :refer [watch-critical-error]]
    )
    
     (:use pseidon.core.registry
           pseidon.core.message
           )
    
    )
  
(set-conf! :zk-url "192.168.56.101")

(def ^:dynamic topic "abctopics")

;(defrecord DataSource [name start stop list-files reader])


(defn read-ftp [{:keys [list-files reader-seq]}]
  "For each file reads each line and sends as a message"
  (let [service (java.util.concurrent.Executors/newFixedThreadPool 10) ]
  (doseq [file (list-files)]
    (.submit service (watch-critical-error (fn [] 
     (doseq [line (reader-seq file)]
      (publish data-queue (->Message (.getBytes line) topic true (System/currentTimeMillis) 1) )
  )))))))

(defn run [] 
 ;start is called in its own thread and does not need to return  
      (let [ftp (reg-get-wait "testftp" 10000)]
         (while true (do (read-ftp ftp) (Thread/sleep 1000) ))        
        )
  )


(defn stop []
  
  )



(register (->Channel "testftpchannel" run stop))

        