(ns plugins.channels.mychannel)
(use '[pseidon.core.app])
(use '[pseidon.core.registry :as r])
(use '[pseidon.core.ds.dummy :as d])
(use '[pseidon.core.queue :as q])
(use '[pseidon.core.message :as m])


 
(defn send-file [file]
     (with-open [rdr ((:reader (r/reg-get "ds-test")) file)]
        (doseq [line (line-seq rdr)]
           (prn "Sending " line) ;/prefixdir/topic-/dateparition
          (q/publish data-queue (m/->Message (.getBytes line) "test" true (System/currentTimeMillis) 1) )
       ))
   )

(defn start [] 
  (prn "Startin sending data")
 (doseq [file ((:list-files (r/reg-get "ds-test" )))]
         (send-file file)
        )
 )

(defn stop []
  (prn "stop my channel")
  )


(r/register (r/->Channel :ch-test start stop))

         
        

