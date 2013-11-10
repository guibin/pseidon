	(ns pseidon.core.fileresource
	   (:require [pseidon.core.tracking :refer [with-txn]]
		     [pseidon.core.utils :refer [fixdelay apply-f]]
		     [pseidon.core.metrics :refer [add-histogram add-gauge update-histogram add-timer add-counter inc-counter dec-counter measure-time]]
	             [clojure.java.io :refer [make-parents]])
	   (:use clojure.tools.logging 
		 pseidon.core.watchdog
		 pseidon.core.conf)
	   
	   (:import (clojure.lang ArityException Agent IFn)
		    (org.apache.hadoop.io.compress CompressionCodec Compressor CompressionOutputStream)
		    (org.apache.hadoop.conf Configurable Configuration)
		    (org.streams.commons.status Status)
		    (org.streams.commons.compression CompressionPool CompressionPoolFactory)
		    (org.streams.commons.compression.impl CompressionPoolFactoryImpl)
		    (java.util.concurrent.atomic AtomicInteger AtomicBoolean)
		    (java.util.concurrent ThreadPoolExecutor SynchronousQueue ThreadPoolExecutor$CallerRunsPolicy TimeUnit)
        (java.io OutputStream File)
	  ))

  
 (def hadoop-conf (Configuration.))

 (def compressor-pool-counters (ref {}))
	(def agent-executor (ref {}))

	(defn get-compressor-pool-counter [lbl]
	  "If the counter exists return else create a new counter, add to the compressor-pool-counters map and return"
	  (dosync 
	    (if-let [v (get compressor-pool-counters lbl nil)]
	      v
	      (let [ v (AtomicInteger. 1) ] ;else create counter and assoc to map
		(add-gauge (str "compressor-pool-" lbl) #(.get v) ) ; create a guage to show the count
		(commute compressor-pool-counters assoc lbl v)
		v))))
	    
	(def ^CompressionPoolFactory compressor-pool-factory (CompressionPoolFactoryImpl. 100 100 (reify Status 
												    (setCounter [self lbl i] 
												      (.set (get-compressor-pool-counter lbl) i)))))

	(def global-on-roll-callbacks (ref {}))

	(defn register-on-roll-callback [^String name ^clojure.lang.IFn f]
	  (info "register global call back " f)
	  "Adds the callback function to a map with the given id the"
	       (dosync
		 (commute global-on-roll-callbacks (fn [m]  (assoc m name f) ))))
 
 
	(defn ^CompressionCodec configure-codec [^CompressionCodec codec ^Configuration hadoop-conf]
	  (if (instance? Configurable codec)
	      (doto codec (.setConf hadoop-conf))
	      codec))

	(defn default-codec [] 
			 (let [codec (configure-codec 
                  (if-let [codec (get-conf2 :default-codec nil)]
                    (-> (Thread/currentThread) .getContextClassLoader (.loadClass (.getName codec)) .newInstance)
                    (org.apache.hadoop.io.compress.GzipCodec.))  hadoop-conf)]
       (info "Using codec " codec " conf " (.getConf codec))
		   codec))

	(defn ^CompressionCodec get-codec [topic]
	  (if-let [ codec (get  (get-conf2 :topic-codecs {}) topic)]
	     (configure-codec codec hadoop-conf)
	     (default-codec) ))


	(defn get-writer-basedir [] 
	  (get-conf2 :writer-basedir "target")
	  )

	(defn get-topic-basedir [topic] 
	  "Returns the base directories for a topic and if not defined uses the writer base dir"
	  (get (get-conf2 :topic-basedirs {}) topic (get-writer-basedir))
	  )


	;returns the complete file name with extension adding an extra '_' suffix
	(defn ^String create-file-name [key, ^CompressionCodec codec]
	   (str key (.getDefaultExtension codec) "_" (System/nanoTime)))

  (def last-nth (fn [xs n] (xs (- (count xs) n))))

	(defn ^String create-rolled-file-name [^File file-name]
	  "Split the filename by . and _, then swap the last two values and joint the list with ."
	  (let [s (clojure.string/split (.getAbsolutePath file-name) #"[_|\.]")
          s-count (count s)]
     (clojure.string/join "." (assoc s (dec s-count) (last-nth s 2) (- s-count 2) (last-nth s 1)))))
 
 (defrecord FileData [^CompressionOutputStream output  ^CompressionCodec codec ^CompressionPool compressor ^File file])

 (defn new-file-data [ & {:keys [output codec compressor file]} ]
      (FileData. output codec compressor file))   
 
 ;contains a map of the agents, each agent in turn contains a map of FileData
 (def master-agent (let [agnt (agent {})] 
                     (set-error-handler! agnt agent-error-handler) 
                     agnt))
 
 
 (def open-files-gauge (add-gauge "pseidon.core.fileresource.open-files" #(count (-> @master-agent (deref)  ) )))

 (defn get-create [m k f create-f & args]
   (if-let [v (get m k)]
     (do 
       (apply f v args)
       m)
     (let [v (apply create-f args)]
       (apply f v args)
       (assoc m k v))))
 
 (defn ^FileData create-file-data [topic file-key ^clojure.lang.IFn writer]
   "Load a codec, compressor, compose the filename, create parent dirs and the file itself, then return an instance of FileData"
    (let [codec (get-codec topic) 
		      ^CompressionPool compressor (-> compressor-pool-factory (.get codec) )
		      file-name (create-file-name (clojure.string/join "/" [(get-topic-basedir topic) file-key]) codec)
          ^File file (File. file-name)]
      
      (make-parents file)
      (.createNewFile file)
      (if (.exists file) (info "Created " file) (throw (java.io.IOException. (str "Failed to create " file)))  )
      
      (if-not compressor
         (throw (RuntimeException. (str "The compressor cannot be null here file: " file " please check the compressor pool" @compressor-pool-counters))))
       
      (let [ fileout (java.io.FileOutputStream. file)
             output (.create compressor fileout 10000 java.util.concurrent.TimeUnit/MILLISECONDS)
             ]
        (if (nil? output)
          (throw (RuntimeException. (str "The output cannot be null here file: " file " please check the compressor pool" @compressor-pool-counters))))
        
        (let [file-data (FileData. output codec compressor file)]
          (info "created " file-data)
          file-data)
        )))
 
 (defn write-to-file [^FileData file-data topic file-key ^IFn writer]
   "Calls the writer on the output of the file-data map"
   (writer (:output file-data)))
 
 (defn send-off-to-topic [topic-agent topic file-key ^IFn writer]
   "Pass the call through get-create and send the result filedata to write-to-file"
   (send-off 
         topic-agent
         get-create
         file-key
         write-to-file
         create-file-data
         topic
         file-key
         writer))
 
 (def ^AtomicBoolean is-shutdown (AtomicBoolean. false))
 
 (defn write [topic file-key ^IFn writer]
   "Pass the call throuh get-create and pass the result (agent {}) to send-off-to-topic"
   (if (.get is-shutdown)
     (throw (RuntimeException. "No writting is allowed after shutdown")))
   
   (send master-agent
         get-create 
         topic
         send-off-to-topic
         (fn [& args] (let [agnt (agent {})] (set-error-handler! agnt agent-error-handler) agnt ))
         topic
         file-key
         writer))
 
 
 (defn close-file [{:keys [^File file ^CompressionOutputStream output ^CompressionPool compressor] :as file-data}]
   (info "closing " file-data)
   (.closeAndRelease compressor output)
   (let [post-roll (vals @global-on-roll-callbacks)
          ^File new-file  (java.io.File. (create-rolled-file-name file ))
           renamed (.renameTo file new-file)
         ]
        (if renamed
          (do  
	           (try 
               (info "closed file " (.getAbsolutePath new-file))
	             (with-txn pseidon.core.tracking/dbspec (doall (pmap (fn [prf] 
                                                                    (apply-f prf new-file)) post-roll)))
	             (catch Exception e (do 
                                    ;if the post-roll messages could not be run, the file is deleted
	                                  (.delete new-file)
	                                  (.delete file)
	                                  (throw e)
	                                  ))))
	         (throw (Exception. (str "Unable to roll file from " file " to " new-file))))
     ))
 
 (defn default-roll-check [{:keys [^java.io.File file ^CompressionOutputStream output]} ]
    "Checks the file size and roll on size"
    (if output (.flush output))
    (or 
      (>= (.length file) (get-conf2 :roll-size 10485760))
      (let [ file-last-modified (.lastModified file) 
             now-ms (System/currentTimeMillis) 
             roll-timeout (get-conf2 :roll-timeout 30000)
             diff-ms (- now-ms file-last-modified)
             ]
           (>= diff-ms roll-timeout)
        )))


 (defn close-topic-file [file-map topic file-key]
   "get s the file-key from file-map, call close and dissoc the key"
   (if-let [file-data (get file-map file-key)]
     (do
       (close-file file-data)
       (dissoc file-map file-key)
       )
     file-map))
 
 (defn send-off-close-topic [topic-agent topic file-key]
   "Pass the call through get-create and send the result filedata to write-to-file"
   (if topic-agent
		   (send-off topic-agent
		         close-topic-file
		         topic
		         file-key
		         )))

  (defn close-roll [topic file-key]
    (send master-agent
          get-create
          topic
          send-off-close-topic
          (fn [& args] nil)
          topic
          file-key))
  
	(defn roll-check []
	   (let [file-agents @master-agent]
	     (doseq [[topic file-map] file-agents]
	       (doseq [[file-key file-data] @file-map]
	         ;do roll check if the file should be rolled send close-roll message
	         (try
	           (if (default-roll-check file-data)
	             (close-roll topic file-key))
	           (catch Exception e (error e e)))
	         ))))
 
   (defn start-services [& {:keys [roll-check-freq] :or {roll-check-freq 5000} }]
     "This method starts the roll check in a separate thread"
     (fixdelay roll-check-freq (roll-check)))
 
   (defn close-all []
     (doseq [[topic file-map] @master-agent]
	       (doseq [[file-key file-data] @file-map]
          (close-roll topic file-key))
         (await-for 500 file-map)
          )
        )
   
   (defn shutdown-services []
     (.set ^AtomicBoolean is-shutdown true)
     (close-all)
     )
          