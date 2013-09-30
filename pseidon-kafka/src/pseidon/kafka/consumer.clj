(ns pseidon.kafka.consumer
    (:require [pseidon.kafka.kafka-util :refer [to-clojure as-properties pipe]]
              [clojure.core.async :refer [chan go <! >!]])
    (:import [kafka.javaapi.consumer ZookeeperConsumerConnector]
              [kafka.consumer ConsumerConfig Consumer KafkaStream ConsumerConnector]
              [kafka.api FetchRequest]
           ))


;;shamelessly copied from https://github.com/pingles/clj-kafka/tree/master/src/clj_kafka
;;the later works with kafka 0.8.1 and this library needs to work with 0.7.2 which is the latest stable release


(defn consumer
  "Uses information in Zookeeper to connect to Kafka. More info on settings
   is available here: http://incubator.apache.org/kafka/configuration.html

   Recommended for using with with-resource:
   (with-resource [c (consumer m)]
     shutdown
     (take 5 (messages c \"test\")))

   Keys:
   zookeeper.connect             : host:port for Zookeeper. e.g: 127.0.0.1:2181
   group.id                      : consumer group. e.g. group1
   auto.offset.reset             : what to do if an offset is out of range, e.g. smallest, largest
   auto.commit.interval.ms       : the frequency that the consumed offsets are committed to zookeeper.
   auto.commit.enable            : if set to true, the consumer periodically commits to zookeeper the latest consumed offset of each partition"
  [m]
  (let [config (ConsumerConfig. (as-properties m))]
    (ZookeeperConsumerConnector. config)))

(defn shutdown
  "Closes the connection to Zookeeper and stops consuming messages."
  [^ConsumerConnector consumer]
  (.shutdown consumer))

(defn topic-map
  [topics]
  (apply hash-map (interleave topics
                              (repeat (Integer/valueOf 1)))))

(defn messages
  "Creates a sequence of KafkaMessage messages from the given topics. Consumes
   messages from a single stream. topics is a collection of topics to consume
   from.
   Optional: queue-capacity. Can be used to limit number of messages held in
   queue before they've been dequeued in the returned sequence. Defaults to
   Integer/MAX_VALUE but can be changed if your messages are particularly large
   and consumption is slow."
  [^ConsumerConnector consumer topics]
  (let [ch (chan 10000)
        stream-map (.createMessageStreams consumer (topic-map topics))]
       (doseq [[topic streams] stream-map]
         (doseq [ stream streams]
           (go
             (doseq [msg (iterator-seq (.iterator stream))]
               (>! ch msg)))))
       ch))
