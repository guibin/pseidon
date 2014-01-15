(ns pseidon.kafka.producer
  )


(defn producer
  "Creates a Producer. m is the configuration
   metadata.broker.list : \"server:port,server:port\""
  [m]
  )

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
    nil))

(defn send-message
  [producer message]
  nil)

(defn send-messages
  [producer messages]
  nil)

