(ns pseidon.kafka.util
  
  (:import [kafka.producer KeyedMessage])
  )


(defn create-message 
  ([^String topic val]
      (KeyedMessage. topic val))
  ([topic key val]
      (KeyedMessage. ^String topic key val)))
