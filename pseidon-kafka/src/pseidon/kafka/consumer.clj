(ns pseidon.kafka.consumer
    (:require
              [clojure.tools.logging :refer [info error]]
              [kafka-clj.consumer  :refer [consumer read-msg close-consumer]]
              [kafka-clj.metrics :refer [ report-consumer-metrics ]]
              [clojure.tools.logging :refer [info]])
    )

(defn lazy-ch [c ]
    (lazy-seq (cons (read-msg c) (lazy-ch c))))

(def close-consumer2 [c]
  (close-consumer))

(defn create-consumer [bootstrap-brokers c topics conf]
  (info "!!!!!!!!!!!!!!!!!!!!!! Bootstrap-brokers " bootstrap-brokers " topics " topics)
  (report-consumer-metrics :csv :freq 10 :dir (get conf :kafka-reporting-dir "/tmp/kafka-consumer-metrics"))
   
  (consumer
              bootstrap-brokers
              topics {:use-earliest true :metadata-timeout 60000 :max-bytes 10485760 :max-wait-time 5000 :min-bytes 1048576
                      :redis-conf (get conf :redis-conf {:redis-host "localhost"}) }))

(defn messages [c ]
  "Returns a lazy sequence that will block when data is not available"
         
    (lazy-ch c))