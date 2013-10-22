(ns pseidon.kafka-hdfs.processor
  
  (:require [pseidon.core.conf :refer [get-conf2]]
            [pseidon.core.queue :refer [publish]]
            [pseidon.core.app :refer [data-queue]]
            [pseidon.core.message :refer [create-message]]
            [pseidon.core.message :refer [create-message]]
            [pseidon.core.registry :refer [register ->Processor reg-get-wait] ]
            [clj-time.coerce :refer [from-long to-long]]
            [clj-time.format :refer [unparse formatter]]
            [pseidon.core.fileresource :refer [write register-on-roll-callback]]
            [pseidon.core.tracking :refer [select-ds-messages mark-run! mark-done! deserialize-message]]
            [clojure.tools.logging :refer [info error]]
            [clojure.data.json :as json]
     )
     (:import [org.apache.commons.lang StringUtils])
  )


(def ^:private dateformat (formatter "yyyyMMddHH"))

(def ^:private ts-parser { :json 
                          (fn [^bytes msg-data path-seq]
                            (reduce (fn [d k] (get d k)) (json/read-str (String. msg-data "UTF-8")) path-seq))
                           :now 
                           (fn [msg-data _]
                             (System/currentTimeMillis))
                          })

(defn- exec-write [^java.io.OutputStream out ^bytes bts]
       (if (nil? bts)
             (error "Receiving null byte messages from  ts " ts)
             (pseidon.util.Bytes/writeln out bts)  
             ))

(def ^:private selected-ts-parser (delay (if-let [parser (get ts-parser (get-conf2 :kafka-hdfs-ts-parser :now) )]
                                    parser
                                    (:now ts-parser))))

(def ^:private ts-parser-args (delay (if-let [parser (get ts-parser (get-conf2 :kafka-hdfs-ts-parser-args nil) )]
                                    parser
                                    nil)))
                                    
;read messages from the logpuller and send to hdfs
(defn exec [ {:keys [bytes-seq ts ds ids] :as msg } ]
  
   (let [id ids
         ^bytes bdata bytes-seq
         [topic partition offset] (StringUtils/split (str id) \:)
         ts ((force selected-ts-parser) bdata (force ts-parser-args))
         key (str topic "_" (unparse dateformat (from-long ts)))]
         (if bdata  
	      (write topic 
               key
               (fn [out] (exec-write out bdata))
               (fn [file_name]
                 ;setting the batchid to done
                 (try
                    (mark-done! ds id (fn[] ))
		    (catch java.sql.BatchUpdateException e (error "ds " ds " id " id " error " e ))))))))

(defn ^:dynamic start []
  (def dsid "pseidon.kafka-hdfs.processor")
  
  ;this will be called when any file written by this channel has been rolled
  ;we send the rolled file to the hdfs plugin and this plugin will take care of sending the file to hdfs
  (register-on-roll-callback dsid (fn [file]
                                                       (let [ds dsid
                                                             topic "hdfs"
                                                             id (.getAbsolutePath file)]
                                                             (mark-run! ds id)
                                                             (publish data-queue (create-message
                                                                          (pseidon.util.Bytes/toBytes "1")
                                                                          ds id topic true (System/currentTimeMillis) 1
                                                                        )))))
  
   ;we recover any messages that the hdfs plugin did not send
   (doseq [{:keys [ds ids ts] :as msg} (map deserialize-message (select-ds-messages dsid))]
       (info "Recovering msg [" msg "]")
       (if msg  
         (publish data-queue (create-message nil ds ids "hdfs" true (to-long ts) 1) )))
  
  )


;register processor with topic solace
(register (->Processor "pseidon.kafka-hdfs.processor" start #() exec))
