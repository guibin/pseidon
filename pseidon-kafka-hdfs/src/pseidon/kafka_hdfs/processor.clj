(ns pseidon.kafka-hdfs.processor
  
  (:require [pseidon.core.conf :refer [get-conf2]]
            [pseidon.core.queue :refer [publish]]
            [pseidon.core.app :refer [data-queue]]
            [pseidon.core.message :refer [create-message]]
            [taoensso.nippy :as nippy]
            [clj-json.core :as json]
            [pseidon.core.metrics :refer [add-meter update-meter] ]
            [pseidon.core.registry :refer [register ->Processor reg-get-wait] ]
            [clj-time.coerce :refer [from-long to-long]]
            [clj-time.format :refer [unparse formatter]]
            [pseidon.core.fileresource :refer [write register-on-roll-callback]]
            [pseidon.core.tracking :refer [select-ds-messages mark-run! mark-done! deserialize-message]]
            [clojure.tools.logging :refer [info error]]
     )
     (:import [org.apache.commons.lang StringUtils])
  )


(def exec-meter (add-meter (str "pseidon.kafka_hdfs.processor" )))

(def ^:private dateformat (formatter "yyyyMMddHH"))

(def ^:private ts-parser { :obj 
                          (fn [msg-data path-seq]
                            	   (reduce (fn [d k] (get d k)) msg-data path-seq))
                          
                           :now 
                           (fn [msg-data _]
                             (System/currentTimeMillis))
                          })

(defn nippy-decoder [^bts bts]
  (nippy/thaw bts))

(defn nippy-encoder [msg]
  (nippy/freeze msg))

(defn ^bytes json-encoder [msg]
  (.getBytes (str (json/generate-string msg)) "UTF-8"))

(defn json-decoder [^bytes bts]
  (json/parse-string (String. bts "UTF-8")))

(defn ^bytes default-encoder [^bytes msg]
  msg)

(defn ^bytes default-decoder [^bytes bts] 
    bts)

(defonce ^:constant decoder-map {:default default-decoder
                                 :nippy nippy-decoder
                                 :json json-decoder})

(defonce ^:constant encoder-map {:default default-encoder
                                 :nippy nippy-encoder
                                 :json json-encoder})
  
(defmacro with-info [s body]
  `(let [x# ~body] (info s " " x#) x#))

(def get-decoder (memoize (fn [log-name]
                            (with-info (str "For topic " log-name " using decoder ")
				                            (let [key1 (keyword (str "kafka-hdfs-" log-name "-decoder"))
				                                  key2 :kafka-hdfs-default-decoder]
				                              (if-let [decoder (get decoder-map (get-conf2 key1 (get-conf2 key2 nil)))]
				                                decoder
				                                default-decoder))))))


(def get-encoder (memoize (fn [log-name]
                            (with-info (str "For topic " log-name " using encoder ")
				                            (let [key1 (keyword (str "kafka-hdfs-" log-name "-encoder"))
				                                  key2 :kafka-hdfs-default-encoder]
				                              (if-let [encoder (get encoder-map (get-conf2 key1 (get-conf2 key2 nil))) ]
				                                encoder
				                                default-encoder))))))

(defn- exec-write [^java.io.OutputStream out ^bytes bts]
       (if (nil? bts)
             (error "Receiving null byte messages from  ts ")
             (pseidon.util.Bytes/writeln out bts)  
             ))

(def  get-ts-parser (memoize (fn [topic]
                               (with-info (str "For topic " log-name " using ts parser ")
                                        (let [key1 (keyword (str "kafka-hdfs-" topic "ts-parser"))
                                              key2 :kafka-hdfs-ts-parser]
                                         (if-let [parser (get ts-parser (get-conf2 key1 (get-conf2 key2 :now) ))]
                                           parser
                                           (:now ts-parser)))))))


(def  get-ts-parser-args (memoize (fn [topic]
                                    (with-info (str "For topic " log-name " using ts parser args ")
		                                        (let [key1 (keyword (str "kafka-hdfs-" topic "ts-parser-args"))
		                                              key2 :kafka-hdfs-ts-parser-args]
		                                         (if-let [parser-args (get-conf2 key1 (get-conf2 key2 ["ts"]) )]
		                                           parser-args
		                                           parser-args))))))

                                    
;read messages from the logpuller and send to hdfs
(defn exec [ {:keys [bytes-seq ts ds ids] :as msg } ]
   (let [id ids
         [topic partition offset] (StringUtils/split (str id) \:)
         bdata ((get-decoder topic) bytes-seq)
         
         ts ((get-ts-parser topic) bdata (get-ts-parser-args topic))
         key (str topic "_" (unparse dateformat (from-long ts)))]
         (if bdata  
	         (write topic 
               key
               (fn [out] (exec-write out ((get-encoder topic) bdata)))
                 ))
         (update-meter exec-meter)))
         

(defn ^:dynamic start []
  (def dsid "pseidon.kafka-hdfs.processor")
   
  ;this will be called when any file written by this channel has been rolled
  ;we send the rolled file to the hdfs plugin and this plugin will take care of sending the file to hdfs
  (let [n-bytes (pseidon.util.Bytes/toBytes "1")]
    (register-on-roll-callback dsid (fn [file]
                                                       (let [ds dsid
                                                             topic "hdfs"
                                                             id (.getAbsolutePath file)]
                                                             (mark-run! ds id)
                                                             (publish data-queue (create-message
                                                                          n-bytes
                                                                          ds id topic true (System/currentTimeMillis) 1
                                                                        ))))))
  
  
   ;we recover any messages that the hdfs plugin did not send
   (doseq [{:keys [ds ids ts] :as msg} (map deserialize-message (select-ds-messages dsid))]
       (info "Recovering msg [" msg "]")
       (if msg  
         (publish data-queue (create-message nil ds ids "hdfs" true (to-long ts) 1) )))
  
  )


;register processor with topic solace
(register (->Processor "pseidon.kafka-hdfs.processor" start #() exec))
