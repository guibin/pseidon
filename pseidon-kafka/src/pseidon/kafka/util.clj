(ns pseidon.kafka.util
  (:require [pseidon.core.conf :refer [get-sub-conf]]
            [clj-kafka.consumer.zk :refer [shutdown consumer messages]]
            [clj-kafka.producer :refer [producer send-messages]]
            [clj-kafka.core :refer [as-properties with-resource]]
            [pseidon.core.registry :refer [create-datasource create-datasink register]]
            [clojure.tools.logging :refer [info error]])  
  (:import [kafka.producer KeyedMessage])
  )


(defn get-kafka-conf []
  (into {} 
        (map (fn [[k v]] [ (if (instance? clojure.lang.Keyword k) (name k) (str k)) v]) (get-sub-conf :kafka))))

(defn create-message 
  ([{:keys [topic k val]}]
      (if k (create-message topic k val)
        (create-message topic val)))
  ([^String topic val]
      (KeyedMessage. topic val))
  ([topic k val]
      (KeyedMessage. ^String topic k val)))

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
                      (messages @c topics))
        ]
      (create-datasource {:name name :run run :stop stop :list-files list-files :reader-seq reader-seq}))))

(defn load-datasink [conf]
  "Returns a DataSink instance that
   when run is called with create a producer once
   stop calls shutdown on the producer
   writer returns a function that takes a list of messages and publishes
          the format of each item must be a KeyedMessage see create-message"
    (let [name "pseidon.kafka.util.datasink"
        p (ref nil) 
        ]
    (letfn [
        (run [] 
             (dosync
               (alter p (fn [v conf] (if-not v (producer conf) v)) conf)))
        
        (stop [] )
        (writer  [messages]
                      (if-not @p (run))
                      (send-messages @p (vec (map create-message messages))))
        ]
      (create-datasink {:name name :run run :stop stop :writer writer}))))

