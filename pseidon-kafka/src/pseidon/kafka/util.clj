(ns pseidon.kafka.util
  (:require [pseidon.core.conf :refer [get-sub-conf]]
            [clj-kafka.consumer.zk :refer [shutdown consumer]]
            [clj-kafka.core :refer [as-properties with-resource]]
            [pseidon.core.registry [create-datasource create-datasink register]])  
  (:import [kafka.producer KeyedMessage])
  )


(defn get-kafka-conf []
  (get-sub-conf :kafka))





