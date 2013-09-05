(ns plugins.processors.test
   (:require 
     [pseidon.core.registry :refer [register ->Processor] ]
     [pseidon.core.fileresource :refer [write]]
     [clj-time.coerce :refer [from-long]]
     [clj-time.format :refer [unparse formatter]]
     [pseidon.core.message :refer [get-bytes-seq get-ids create-message]]
     [clojure.tools.logging :refer [info error]]
     [pseidon.core.tracking :refer [select-ds-messages mark-run! mark-done!]]
     [pseidon.core.queue :refer [publish]]
     [pseidon.core.app :refer [data-queue]]
     )
   )

(import '(java.io OutputStream))

;(defrecord Message [bytes-f ^String topic ^boolean accept ^long ts ^int priority] 
;(defn write [topic key ^clojure.lang.IFn writer]

(def ^:dynamic dateformat (formatter "yyyyMMddHH"))

;this method will be called when a new message for topic test arrives at the queue
(defn ^:dynamic exec [ {:keys [topic ts ds] :as msg } ]
  (defn exec-write [out bts]
       (if (or (nil? bts) (< (count bts) 1) )
             (error "Receiving null byte messages from " topic " ts " ts)
             (pseidon.util.Bytes/writeln out bts)  
             )
       )
  
  ;we call mark-done! and pass it a clojure that will write all of the bytes for the message.
  ;if any error the mark-done will roll back any status flags set.
  (write topic 
               (str "ftp_1_hr_" (unparse dateformat (from-long ts)))
               (fn [out] (doseq [bts (get-bytes-seq msg) ] (exec-write out bts)))
               (fn [file] 
                 (mark-done! ds (get-ids msg) (fn [] 
                                                (let [ds "testprocess-hdfs"
                                                      topic "hdfs"
                                                      id (.getAbsolutePath file)]
		                                                (mark-run! ds id)
		                                                (publish data-queue (create-message  nil
																                                                         ds
																                                                         id
																                                                         topic
																                                                         true 
																                                                         (System/currentTimeMillis) 1))))))))

(defn ^:dynamic stop []
  (prn "Stop processing")
  )

(defn ^:dynamic start []
  (doseq [msg (select-ds-messages "testprocess-hdfs")]
       (info "Recovering msg [" msg "]")
       (publish data-queue msg))
  (prn "Starting test processors"))

;register processor with topic test
(register (->Processor "test" start stop exec))
(register (->Processor "abctopics" start stop exec))
 
