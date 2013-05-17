(ns plugins.processors.test)
(use '[pseidon.core.registry :as r])

;this method will be called when a new message for topic test arrives at the queue
(defn exec [ msg ]
  
  (prn "Processing message " msg)
      ; topics, bytes, priority, ts
      ; msg.topic  = useractions
      ; if msg == click
      ;      getDataSink("hdfs").sendOff( new MessageMetaData(msg.bytes, [clicks], ..)) 
      ; elseif msg == impression
      ;      getDataSink("hdfs").sendOff( new MessageMetaData(bytes, [impressions], ..))
      ; elseif msg == pixels
      ;      getDataSink("hdfs").sendOff( new MessageMetaData(bytes, [clicks], ..))
      
  )


(defn stop []
  (prn "Stop processing")
  )

(defn start []
  (prn "Starting test processors")
  )

;register processor with topic test
(r/register-processor (r/->Processor "test" start stop exec))
 
