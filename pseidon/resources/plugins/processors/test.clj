(ns plugins.processors.test
   (:require 
     [pseidon.core.registry :refer [register ->Processor] ]
     [pseidon.core.fileresource :refer [write]]
     [clj-time.coerce :refer [from-long]]
     [clj-time.format :refer [unparse formatter]]
     [pseidon.core.message :refer [get-bytes]]
     [clojure.tools.logging :refer [info]]
     )
   )

(import '(java.io OutputStream))

;(defrecord Message [bytes-f ^String topic ^boolean accept ^long ts ^int priority] 
;(defn write [topic key ^clojure.lang.IFn writer]

(def ^:dynamic dateformat (formatter "yyyy-MM-dd-HH"))

;this method will be called when a new message for topic test arrives at the queue
(defn exec [ {:keys [topic ts] :as msg } ]
  (let [ bts (get-bytes msg)]
  (write topic 
         (unparse dateformat (from-long ts))
         (fn [out] (pseidon.util.Bytes/write out bts))
         )
  )
 )
         
(defn stop []
  (prn "Stop processing")
  )

(defn start []
  (prn "Starting test processors")
  )

;register processor with topic test
(register (->Processor "test" start stop exec))
(register (->Processor "abctopics" start stop exec))
 
