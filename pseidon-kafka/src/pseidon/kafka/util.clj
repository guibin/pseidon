(ns pseidon.kafka.util
  (:require [pseidon.core.conf :refer [get-sub-conf]]
            [pseidon.kafka.consumer :refer [messages create-consumer close-consumer2]]
            [pseidon.kafka.producer :refer [producer send-messages send-message close-producer]]
            [pseidon.core.registry :refer [create-datasource create-datasink register]]
            [clojure.tools.logging :refer [info error]]
            [pseidon.core.metrics :refer [add-meter update-meter]]
            [taoensso.nippy :as nippy]
            [clojure.core.async :refer [chan thread <!! >!!]]
            [pseidon.core.watchdog :refer [handle-critical-error]])
  )



(def kafka-datasink-meter (add-meter "pseidon.kafka.util.datasink.publish"))

(defn get-kafka-conf []
   (get-sub-conf :kafka))


(defn load-datasource [conf]
  "Returns a DataSource instance that
   when run is called it will only create a consumer instance the first time its called
   stop will call shutdown on the consumer
   list-files returns nil
   reader-seq will returns a function (fn [&topics]) and when called returns a sequence of messages
  "
  (prn "conf " conf)
  (let [name "pseidon.kafka.util.datasource"
        bootstrap-brokers (get conf :bootstrap-brokers)
        c-ref (atom nil)
        ]
    (letfn [
        (run [] 
              )
        (stop []
              (if (nil? @c-ref)
                 (close-consumer2 @c-ref)))
        (list-files  [] )
        (reader-seq  [ & topics]
                     (if (not @c-ref)
                        (swap! c-ref #(if (nil? %) (create-consumer bootstrap-brokers topics conf) %)))
                     (messages @c-ref))
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
         p    (producer conf)]

    (letfn [
        (run [])
        (stop []
              (close-producer p))

        (writer-f  [messages]
                      (if (and (coll? messages) (not (map? messages)))
                             (do (update-meter kafka-datasink-meter (count messages))
                                 (send-messages p messages))
                             (do (update-meter kafka-datasink-meter)
                                 (send-message p messages))))
        ]

      (create-datasink {:name name :run run :stop stop :writer writer-f}))))

