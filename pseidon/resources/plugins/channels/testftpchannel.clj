(ns plugins.channels.testftpchannel
  
  (:require 
      [pseidon.core.datastore :refer [get-data-long]]
      [pseidon.core.conf :refer [get-conf2]]
    )
    
     (:use pseidon.core.registry)
    
    )
  
(defn ^:dynamic topic "abctopics")

;(defrecord DataSource [name start stop list-files reader])

(defn read-ftp [{:keys [list-files reader]}]
  "For each file reads each line and sends as a message"
  (doseq [file (list-files)]
    (doseq [line (line-seq (reader file))]
      (q/publish data-queue (m/->Message (.getBytes line) topic true (System/currentTimeMillis) 1) )
  )))

(defn run [] 
 ;start is called in its own thread and does not need to return  
      (let [ftp (reg-get-wait "testftp" 10000)]
         (while true (do (read-ftp ftp) (Thread/sleep 1000) ))        
        )
  )


(defn stop []
  
  )