(ns plugins.channels.mychannel)
(use '[pseidon.core.app])
(use '[pseidon.core.registry :as r])
(use '[pseidon.core.ds.dummy :as d])
(use '[pseidon.core.queue :as q])
(import '[org.streams.streamslog.log.file MessageMetaData])

;MessageMetaData(msg: Array[Byte], topics: Array[String], accept: Boolean = true, 
;  ts:Long=System.currentTimeMillis())

(def ds  (r/get-ds "test"))

(defn send-file [file]
     (with-open [rdr ((:reader ds) file)]
        (doseq [line (line-seq rdr)]
           (prn "Sending " line)
          (q/publish data-queue (MessageMetaData. (.getBytes line) (into-array ["test"]) true (System/currentTimeMillis) ) )
       ))
   )


 (doseq [file  ( (:list-files ds ) ) ]
         (send-file file)
        )
 
         
        

