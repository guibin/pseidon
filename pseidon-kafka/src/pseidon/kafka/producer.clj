(ns pseidon.kafka.producer
  (require [kafka-clj.client :refer [create-connector send-msg close]]))


(defn producer
  "Creates a Producer. m is the configuration
   metadata.broker.list : \"server:port,server:port\""
  [conf]
  (create-connector (get conf "bootstrap-brokers") {}))

(defn close-producers [p]
  (close p))

(defn send-message
  [producer message]
   (send-msg producer (:topic message) message))


(defn send-messages
  [producer messages]
  (doseq [msg messages]
    (send-message producer msg)))

