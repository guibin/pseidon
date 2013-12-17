(ns pseidon.kafka.producer
  (:require [pseidon.kafka.kafka-util :refer [as-properties]])
  (:import [kafka.javaapi.producer Producer]
           [kafka.producer ProducerConfig KeyedMessage]
           [java.util List]
           
           [kafka.message Message] )
  )


(defn producer
  "Creates a Producer. m is the configuration
   metadata.broker.list : \"server:port,server:port\""
  [m]
  (Producer. (ProducerConfig. (as-properties m))))

(defprotocol ToBytes
  (toBytes [this]))

(extend-protocol ToBytes
  String
  (toBytes [this] (.getBytes this "UTF-8"))
  #=(java.lang.Class/forName "[B")
  (toBytes [this] this)
  
  )

(defn message
  ([topic value] 
    (message topic "1" value))
  ([topic key value] 
                     (KeyedMessage. topic key value)))

(defn send-message
  [^Producer producer ^KeyedMessage message]
  (.send producer message))

(defn send-messages
  [^Producer producer ^List messages]
  (.send producer messages))

