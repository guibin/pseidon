(ns pseidon.kafka.consumer
    (:require [pseidon.kafka.kafka-util :refer [to-clojure as-properties pipe]]
              [pseidon.core.watchdog :refer [handle-critical-error]]
              [clojure.core.async :refer [chan >!! alts!! thread]]
              [clojure.tools.logging :refer [info error]])
    (:import [kafka.javaapi.consumer ZookeeperConsumerConnector]
              [kafka.consumer ConsumerConfig Consumer KafkaStream ConsumerConnector ConsumerIterator]
              [kafka.api FetchRequest]
              [pseidon.kafka.util KafkaStreamsHelper]
              
           ))

(defn to-string-conf [m]
  (into {} 
        (map (fn [[k v]] [ (if (instance? clojure.lang.Keyword k) (name k) (str k)) v]) m)))


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
  (let [config (ConsumerConfig. (as-properties (to-string-conf m)))]
    (ZookeeperConsumerConnector. config)))

(defn shutdown
  "Closes the connection to Zookeeper and stops consuming messages."
  [^ZookeeperConsumerConnector consumer]
  (.shutdown consumer))

(defn topic-map
  [topics]
  (apply hash-map (interleave topics
                              (repeat (Integer/valueOf 1)))))

;returns kafka.message.MessageAndMetadata[K, V](key: K, message: V, topic: String, partition: Int, offset: Long)
"
while (!Thread.interrupted()) {
								try {
									final MessageAndMetadata<byte[], byte[]> obj = it
											.next();
									queue.put(obj);
								} catch (java.util.NoSuchElementException ne) {
									Thread.sleep(500);
								}
"
(defn lazy-channels [chs]   (lazy-seq (cons (let [ [v _] (alts!! chs)] v) (lazy-channels chs))))
(defn messages
  "Returns a lazy sequence that will block when data is not available"
  [^ZookeeperConsumerConnector consumer & topics]
  ;List<KafkaStream<byte[], byte[]>> list = flatten(conn, topicMap);
  (let [streams (KafkaStreamsHelper/flatten consumer (topic-map topics))
				chs		  (doall 
                     (for [^KafkaStream stream streams]
									       (let [ch (chan)]
                            (thread
                                   (try
													           (let [^ConsumerIterator it (.iterator stream)]
													              (while (not (Thread/interrupted))
	                                           (try 
													                      (>!! ch (.next it))
	                                              (catch java.util.NoSuchElementException ne
	                                                     (Thread/sleep 100)))))
                                       (catch InterruptedException ie (doto (Thread/currentThread) .interrupt))
                                       (catch Exception e (handle-critical-error e "Error while reading from kafka "  ))))
                              ch)))]
           (map to-clojure 
                (lazy-channels chs))))


