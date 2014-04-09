(ns pseidon.kafka.consumer
    (:require
              [clojure.tools.logging :refer [info error]]
              [kafka-clj.consumer  :refer [consumer read-msg close-consumer] :as kfk]
              [kafka-clj.metrics :refer [ report-consumer-metrics ]]
              [clojure.tools.logging :refer [info]])
    )

(defn lazy-ch [c ]
    (lazy-seq (cons (read-msg c) (lazy-ch c))))

(defn close-consumer2 [c]
  (close-consumer c))

(defn add-topic [consumer topic]
  (kfk/add-topic consumer topic))

(defn remove-topic [consumer topic]
  (kfk/remove-topic consumer topic))

(defn create-consumer [bootstrap-brokers topics conf]
  (info "!!!!!!!!!!!!!!!!!!!!!! Bootstrap-brokers " bootstrap-brokers " topics " topics)
  (report-consumer-metrics :csv :freq 10 :dir (get conf :kafka-reporting-dir "/tmp/kafka-consumer-metrics"))

  (consumer
              bootstrap-brokers
              topics (merge conf
                        {:use-earliest true :metadata-timeout 60000

                        :redis-conf (get conf :redis-conf {:redis-host "localhost"}) })))

(defn messages [c ]
  "Returns a lazy sequence that will block when data is not available"

    (lazy-ch c))
