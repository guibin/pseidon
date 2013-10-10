(ns pseidon.kafka.util
  (:require [pseidon.core.conf :refer [get-sub-conf]]
            [pseidon.kafka.consumer :refer [shutdown consumer messages]]
            [pseidon.kafka.producer :refer [producer send-messages send-message message]]
            [pseidon.kafka.kafka-util :refer [as-properties with-resource]]
            [pseidon.core.registry :refer [create-datasource create-datasink register]]
            [clojure.tools.logging :refer [info error]]
            [pseidon.core.metrics :refer [add-meter update-meter]])
   (:import 
            [kafka.message Message])
  )

(def kafka-datasink-meter (add-meter "pseidon.kafka.util.datasink.publish"))

(defn get-kafka-conf []
  (into {} 
        (map (fn [[k v]] [ (if (instance? clojure.lang.Keyword k) (name k) (str k)) v]) (get-sub-conf :kafka))))

(defn create-message 
  ([{:keys [topic k ^bytes val] :as msg}]
      (if k (create-message topic k val)
        (create-message topic val)))
  ([^String topic ^bytes val]
      (message topic [(Message. val)] ))
  ([topic k ^bytes val]
      (message ^String topic k [(Message. val)])))

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
                      (if (and (coll? messages) (not (map? messages)))
                             (do (update-meter kafka-datasink-meter (count messages)) 
                                 (send-messages @p (vec (map create-message messages)))) 
                             (do (update-meter kafka-datasink-meter) 
                                 (send-message @p (create-message messages)))))
        ]
      (create-datasink {:name name :run run :stop stop :writer writer}))))

