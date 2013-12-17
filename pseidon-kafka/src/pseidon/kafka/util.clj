(ns pseidon.kafka.util
  (:require [pseidon.core.conf :refer [get-sub-conf]]
            [pseidon.kafka.consumer :refer [shutdown consumer messages]]
            [pseidon.kafka.producer :refer [producer send-messages send-message message]]
            [pseidon.kafka.kafka-util :refer [as-properties with-resource]]
            [pseidon.core.registry :refer [create-datasource create-datasink register]]
            [clojure.tools.logging :refer [info error]]
            [pseidon.core.metrics :refer [add-meter update-meter]]
            [taoensso.nippy :as nippy]
            [clojure.core.async :refer [chan thread <!! >!!]]
            [pseidon.core.watchdog :refer [handle-critical-error]])
   (:import 
            [kafka.message Message]
            )
  )



(def kafka-datasink-meter (add-meter "pseidon.kafka.util.datasink.publish"))

(defn to-string-conf [m]
  (into {} 
        (map (fn [[k v]] [ (if (instance? clojure.lang.Keyword k) (name k) (str k)) v]) m)))
        
(defn get-kafka-conf []
  (to-string-conf (get-sub-conf :kafka)))

(defn create-message 
  ([{:keys [topic k val] :as msg}]
      (if k (create-message topic k val)
        (create-message topic val)))
  ([^String topic val]
      (message topic val))
  ([topic k val]
      (message ^String topic k val)))

(defn load-datasource [conf]
  "Returns a DataSource instance that
   when run is called it will only create a consumer instance the first time its called
   stop will call shutdown on the consumer
   list-files returns nil
   reader-seq will returns a function (fn [&topics]) and when called returns a sequence of messages
  "
  (let [name "pseidon.kafka.util.datasource"
        c (ref nil) 
        ]
    (letfn [
        (run [] 
              (dosync
                (alter c (fn [v conf] (if-not v (consumer conf) v)) conf )))
        (stop []
               (try 
                 (if-let [x @c] (shutdown x))
                 (catch Exception e (error e))))
        (list-files  [] )
        (reader-seq  [ & topics]
                      (if-not @c (run))
                      (apply messages @c topics))
        ]
      (create-datasource {:name name :run run :stop stop :list-files list-files :reader-seq reader-seq}))))

(defn load-datasink [conf]
  "Returns a DataSink instance that
   when run is called with create a producer once
   stop calls shutdown on the producer
   writer returns a function that takes a list of messages and publishes
          the format of each item must be a KeyedMessage see create-message
   "
    ;here we use N producers to improve kafka send performance    
    (let [name "pseidon.kafka.util.datasink"
          producer-count (get conf "producers" 6)
          producers (vec (repeatedly producer-count (partial producer conf))) ;create n producers
          kafka-ch (chan 1000) 
        ]
      
      
      (info "Producers: " (count producers)) 
    (letfn [
        (run [])
        (stop [] 
              (doseq [p producers]
                (.close p)))
        
        (writer [messages]
                ;called by client, will block if the channel is full
                (>!! kafka-ch messages))
        
        (writer-f  [p messages]
                   ;called by the asyn thread below, to write to kafka
                      (if (and (coll? messages) (not (map? messages)))
                             (do (update-meter kafka-datasink-meter (count messages)) 
                                 (send-messages p (vec (map #(apply create-message %) messages)))) 
                             (do (update-meter kafka-datasink-meter) 
                                 (send-message p (create-message messages)))))
        ]
      
      ;thread that will read from kafka-ch and asynchronously send to kafka
      (thread 
        (try
          (loop [n 0] 
            (if-let [msg (<!! kafka-ch)]
              (writer-f (nth producers (rem n producer-count))  msg))
            (recur (if (= n Long/MAX_VALUE) 0 (inc n))))
          (catch Exception e (handle-critical-error e e) )))
      
      (create-datasink {:name name :run run :stop stop :writer writer}))))

