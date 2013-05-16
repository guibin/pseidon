(ns plugins.channels.mychannel)
(use '[pseidon.core.app])
(use '[pseidon.core.registry :as r])
(use '[pseidon.core.ds.dummy :as d])
(use '[pseidon.core.queue :as q])
(import '[org.streams.streamslog.log.file MessageMetaData])

;MessageMetaData(msg: Array[Byte], topics: Array[String], accept: Boolean = true, 
;  ts:Long=System.currentTimeMillis())

 
(defn send-file [file]
     (with-open [rdr ((:reader (r/get-ds "test")) file)]
        (doseq [line (line-seq rdr)]
           (prn "Sending " line)
          (q/publish data-queue (MessageMetaData. (.getBytes line) (into-array ["test"]) true (System/currentTimeMillis) 1) )
       ))
   )

(defn start [] 
 (doseq [file ((:list-files (r/get-ds "test" )))]
         (send-file file)
        )
 )

(defn stop []
  (prn "stop my channel")
  )


(r/register-channel {:start start :stop stop})

         
        

