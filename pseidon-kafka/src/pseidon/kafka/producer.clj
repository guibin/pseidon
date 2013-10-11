(ns pseidon.kafka.producer
  (:require [pseidon.kafka.kafka-util :refer [as-properties]])
  (:import [kafka.javaapi.producer Producer ProducerData]
           [kafka.producer ProducerConfig ]
           [java.util List]
           [kafka.message Message] )
  )

;;shamelessly copied from https://github.com/pingles/clj-kafka/tree/master/src/clj_kafka
;;the later works with kafka 0.8.1 and this library needs to work with 0.7.2 which is the latest stable release
(defn producer
  "Creates a Producer. m is the configuration
   metadata.broker.list : \"server:port,server:port\""
  [m]
  (Producer. (ProducerConfig. (as-properties m))))

(defprotocol ToBytes
  (toBytes [this]))

(extend-protocol ToBytes
  String
  (toBytes [this] (.getBytes this))
  #=(java.lang.Class/forName "[B")
  (toBytes [this] this)
  
  )

(defn message
  ([topic value] (message topic nil value))
  ([topic key value] 
                     (ProducerData. topic key [(Message. ^bytes (toBytes value))])))

(defn send-message
  [^Producer producer ^ProducerData message]
  (.send producer message))

(defn send-messages
  [^Producer producer ^List messages]
  (.send producer messages))

