(ns pseidon.kafka-hdfs.channel
  (:require [pseidon.core.conf :refer [get-conf2]]
            [pseidon.core.queue :refer [publish]]
            [pseidon.core.app :refer [data-queue]]
            [pseidon.core.message :refer [create-message]]
            [pseidon.core.metrics :refer [add-meter update-meter] ]
            [pseidon.core.registry :refer [register ->Channel reg-get-wait] ]
            [pseidon.core.watchdog :refer [watch-critical-error]]
            [pseidon.core.tracking :refer [select-ds-messages mark-run! mark-done! deserialize-message]]
	    [pseidon.core.fileresource :refer [write register-on-roll-callback]]
     	    [clojure.tools.logging :refer [info error]]
	    [clj-time.coerce :refer [from-long to-long]]
            [clj-time.format :refer [unparse formatter]]
	)
  (:import (java.util.concurrent Executors TimeUnit)
           (java.util.concurrent.atomic AtomicBoolean) )
  )


(def ^:dynamic ch-dsid "pseidon.kafka-hdfs.channel")
(def ^:dynamic service (Executors/newCachedThreadPool))

(def kafka-reader  (delay (let [{:keys [reader-seq]} (reg-get-wait "pseidon.kafka.util.datasource" 10000)] reader-seq)))

(defn ^:dynamic process-topics [topics]
  "Start in infinite loop that will read the current batch ids and send to the hdfs topic
  "
  (let [consume-meter-map (into {} (map (fn [n] [n (add-meter (str "pseidon.kafka_hdfs.channel-" n))]) topics))]
	  (while (not (Thread/interrupted))
	    (let [rdr-seq (apply (force kafka-reader) topics)]
	      (doseq [{:keys [offset topic partition value] :as msg } rdr-seq]
	        (let [msg-id (str topic ":" partition ":" offset)]
	          (update-meter (get consume-meter-map topic))
	          (try
             (do
               (mark-run! ch-dsid msg-id)   ; mark message as run, the processor will mark as done
               (publish data-queue (create-message value ch-dsid msg-id "pseidon.kafka-hdfs.processor" true (System/currentTimeMillis) 1))
               )
             (catch java.sql.BatchUpdateException e (info "ignore duplicate message " msg-id)))))))))


(defn ^:dynamic channel-init []
  
  ;this will be called when any file written by this channel has been rolled
  ;we send the rolled file to the hdfs plugin and this plugin will take care of sending the file to hdfs
  (register-on-roll-callback ch-dsid (fn [file]
                                                       (let [ds ch-dsid
                                                             topic "hdfs"
                                                             id (.getAbsolutePath file)]
                                                             (mark-run! ds id)
                                                             (publish data-queue (create-message
                                                                          (pseidon.util.Bytes/toBytes "1")
                                                                          ds id topic true (System/currentTimeMillis) 1
                                                                        )))))
  
   ;we recover any messages that the hdfs plugin did not send
   (doseq [{:keys [ds ids ts] :as msg} (map deserialize-message (select-ds-messages ch-dsid))]
       (info "Recovering msg [" msg "]")
       (if msg  
         (publish data-queue (create-message nil ds ids "pseidon.kafka-hdfs.processor" true (to-long ts) 1) )))
  )

(defn ^:dynamic load-channel []
  (let [topics (get-conf2 :kafka-hdfs-topics [])]
  {:start (fn [] 
              (channel-init)
              (prn "Using logs " topics)
              (.submit service (watch-critical-error process-topics topics)))
   
   :stop (fn []
           (.shutdown service)
           (.awaitTermination service 10000 TimeUnit/MILLISECONDS)
           (.shutdownNow service))
   }))

(let [{:keys [start stop]} (load-channel)]
       (register (->Channel ch-dsid start stop)))  
           

  



