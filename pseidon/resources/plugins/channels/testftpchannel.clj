(ns plugins.channels.testftpchannel
  
  (:require 
      [pseidon.core.datastore :refer [get-data-long]]
      [pseidon.core.conf :refer [get-conf2]]
      [pseidon.core.queue :refer [publish]]
      [pseidon.core.app :refer [data-queue]]
      [pseidon.core.conf :refer [set-conf!]]
      [pseidon.core.watchdog :refer [watch-critical-error]]
      [pseidon.core.message :refer [create-message]]
      [clojure.tools.logging :refer [info error]]
    )
    
     (:use pseidon.core.registry
           )
    
    )
  
(set-conf! :zk-url "192.168.56.101")

(def ^:dynamic topic "abctopics")

;(defrecord DataSource [name start stop list-files reader])


(defn read-ftp [{:keys [list-files reader-seq] :as m}]
  "For each file reads each line and sends as a message"
  (let [service (java.util.concurrent.Executors/newFixedThreadPool 1) ]
   (if (nil? m) (throw (Exception. "The message cannot be nil here")))
   (if-let [files (list-files)]
    (do
      
    (info "Reading files " files)
	  (doseq [file files]
	    (.submit service 
	       (watch-critical-error 
	          (fn [] 
	             (doseq [line (reader-seq file)]
	                  (if line (publish data-queue (create-message (.getBytes line) topic true (System/currentTimeMillis) 1) ) )
	              )
	            )
	          )
	       )
	    )
   )
   )
    (doto service .shutdown (.awaitTermination Long/MAX_VALUE java.util.concurrent.TimeUnit/MILLISECONDS))
    ;(while true (Thread/sleep 10000))
  )
 )

(defn run [] 
 ;start is called in its own thread and does not need to return  
      (let [ftp (reg-get-wait "testftp" 10000)]
         (while true (do (read-ftp ftp) (Thread/sleep 1000) ))        
        )
  )


(defn stop []
  
  )



(register (->Channel "testftpchannel" run stop))

        