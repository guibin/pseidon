(ns pseidon.kafka.consumer
    (:require 
              [clojure.tools.logging :refer [info error]]
              [kafka-clj.consumer  :refer [consumer read-msg]]
              [clojure.tools.logging :refer [info]])
    )

(defn lazy-ch [c]
  (lazy-seq (cons (read-msg c) (lazy-ch c))))
       


(defn messages [bootstrap-brokers topics conf]
  "Returns a lazy sequence that will block when data is not available"
  (info "!!!!!!!!!!!!!!!!!!!!!! Bootstrap-brokers " bootstrap-brokers " topics " topics)
  
  (let [c (consumer 
              bootstrap-brokers
              topics {:use-earliest true :max-bytes 1073741824 :metadata-timeout 60000
                      :redis-conf (get conf :redis-conf {:redis-host "localhost"}) })]
    (lazy-ch c)))


