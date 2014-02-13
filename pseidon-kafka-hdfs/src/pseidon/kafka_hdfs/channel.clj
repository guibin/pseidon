(ns pseidon.kafka-hdfs.channel
  (:require [pseidon.core.conf :refer [get-conf2]]
            [pseidon.core.queue :refer [publish]]
            [pseidon.core.message :refer [create-message]]
            [pseidon.core.metrics :refer [add-meter update-meter] ]
            [pseidon.core.registry :refer [register ->Channel reg-get-wait] ]
            [pseidon.core.watchdog :refer [watch-critical-error]]
            [pseidon.core.tracking :refer [select-ds-messages mark-run! mark-done! deserialize-message]]
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
	      (doseq [{:keys [offset topic partition bts] } rdr-seq]
	        (let [msg-id (str topic ":" partition ":" offset)]
	          (update-meter (get consume-meter-map topic))
	          (try
             (do
               (publish "pseidon.kafka-hdfs.processor" (create-message bts ch-dsid msg-id topic true (System/currentTimeMillis) 1)))
             (catch java.sql.BatchUpdateException e (info "ignore duplicate message " msg-id)))))))))


(defn ^:dynamic channel-init []
  
  ;this will be called when any file written by this channel has been rolled
  ;we send the rolled file to the hdfs plugin and this plugin will take care of sending the file to hdfs

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
           

  



