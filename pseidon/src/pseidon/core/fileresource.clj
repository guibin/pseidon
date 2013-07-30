(ns pseidon.core.fileresource
   (:require [pseidon.core.tracking :refer [with-txn]])
   (:use clojure.tools.logging 
         pseidon.core.watchdog
         pseidon.core.conf
         pseidon.core.wal)
  )

(import '(org.streams.commons.compression CompressionPool CompressionPoolFactory))
(import '(org.streams.commons.compression.impl CompressionPoolFactoryImpl))

;post-roll is a sequence of functions that will be applied only after the file is rolled
(defrecord FileRS [name output codec compressor file walfile post-roll])
(defrecord TopicConf [key codec])


(def compressor-pool-factory (CompressionPoolFactoryImpl. 100 100 nil))

(def file-resource-exec-service (ref nil))

(def fileMap (ref {}))

(def gzip-codec (org.apache.hadoop.io.compress.GzipCodec.))

;find codec from codec map otherwise return the GzipCodec
(defn get-codec [topic]
   (get  (get-conf2 :topic-codecs {}) topic  gzip-codec)
  )
(defn get-writer-basedir [] 
  (get-conf2 :writer-basedir "target")
  )

(defn get-topic-basedir [topic] 
  "Returns the base directories for a topic and if not defined uses the writer base dir"
  (get (get-conf2 :topic-basedirs {}) topic (get-writer-basedir))
  )


;returns the complete file name with extension adding an extra '_' suffix
(defn create-file-name [key, codec]
   (str key (.getDefaultExtension codec) "_" (System/nanoTime)))


(defn create-rolled-file-name [^String file-name]
  "Split the filename by . and _, then swap the last two values and joint the list with ."
  (def last-nth (fn [xs n] (xs (- (count xs) n))))

  (let [s (clojure.string/split file-name #"[_|\.]")
        s-count (count s)
        ]
  
     (clojure.string/join "." (assoc s (dec s-count) (last-nth s 2) (- s-count 2) (last-nth s 1) ) )
     
  ))


 ;alter the fileMap to contain the  agent
 (defn add-agent [topic key]
   
  (let [codec (get-codec topic) 
        compressor (-> compressor-pool-factory (.get codec) )
        file-name (create-file-name (clojure.string/join "/" [(get-topic-basedir topic) key]) codec)
        agnt 
           (agent 
             (->FileRS 
               file-name 
               nil 
               codec 
               compressor 
               (java.io.File. file-name) 
               (create-walfile (clojure.string/join "-" [file-name "-wal"] ))
               [] ;no post roll yet
               )
             )
           ]
        (set-error-handler! agnt agent-error-handler)
        (alter fileMap (fn [p] (assoc p key agnt )))
        agnt
  ))
 
;get an agent and if it doesnt exist create one with a FileRS instance as value
(defn get-agent [topic key]
 (dosync (if-let [agnt (get @fileMap key) ]  agnt (add-agent topic key) )
  ))

;create a file using the codec
(defn create-file [^java.io.File file ^org.apache.hadoop.io.compress.CompressionCodec codec compressor]
       (if-let [parent (.getParentFile file)] (.mkdirs parent) (info "File has no parent " file) )
       (.createNewFile file)
       (if (.exists file) (info "Created " file) (throw (java.io.IOException. (str "Failed to create " file)))  )
       (let [ fileout (java.io.FileOutputStream. file)
              output  
              (.create compressor fileout 1000 java.util.concurrent.TimeUnit/MILLISECONDS)
             ]
         output
        
      ))
     

(defn write-to-frs [^clojure.lang.IFn writer ^clojure.lang.IFn post-roll-fn ^FileRS frs]
  (let [codec (:codec frs) 
        frs-t 
             (if-not 
                 (nil? (:output frs))
                  
               (if (nil? post-roll-fn) 
                 frs 
                 (assoc frs :post-roll (conj (:post-roll frs) post-roll-fn
                                             ) 
                        ) 
                 ) ; we add the post roll function
               (->FileRS (:name frs) 
                         (create-file 
                           (:file frs) 
                           codec 
                           (:compressor frs) 
                           )  
                         codec 
                         (:compressor frs) 
                         (:file frs) 
                         (:walfile frs)
                         (if (nil? post-roll-fn) (:post-roll frs) (conj (:post-roll frs) post-roll-fn))
                         )
               )
        ]
    
      (writer (:output frs-t))
      
      ;we always return a FileRS instance
      frs-t))

; will do an async function send to that will call the writer (writer output-stream)
; post-roll-fn can be nill if not its applied only when the FileRS is rolled.
; this means each FileRS will have a collection of post-roll-fn(s) and each is applied on roll-over.
; if any error on these functions the rolled file is deleted and the status in the tracking db setup back.
(defn write [topic key ^clojure.lang.IFn writer ^clojure.lang.IFn post-roll-fn]
   (let [agent ((watch-critical-error get-agent topic key))]
     (dosync (send-off agent (watch-critical-error write-to-frs writer post-roll-fn) ) )
    )
  )

(defn close-roll-agent [^FileRS frs]
  (when (and (not (nil? frs)) (not (nil? (:output frs) ) ) ) 
    (.closeAndRelease (:compressor frs) (:output frs) ) 
    ;find rename the file by removing the last \.([a-zA-Z0-9])+_([0-9]+) piece of the filename and appending (group2).(group1)
  
	  (let [
	        post-roll (:post-roll frs)
	        file (:file frs)  
	        new-file  (java.io.File. (create-rolled-file-name (:name frs) ))
	        renamed (.renameTo file new-file)
	        ]
	     (if renamed 
	       (do 
	         (info "File " new-file " created") 
	         (close-destroy (:walfile frs))
	           (try 
	             (with-txn (doseq [prf post-roll] (prf)))
	             (catch Exception e (do 
                                    ;if the post-roll messages could not be run, the file is deleted
	                                  (.delete new-file)
	                                  (.delete file)
	                                  (throw e)
	                                  ) 
	               ) 
	             )
	         )
	       (throw (Exception. (str "Unable to roll file from " file " to " new-file)))
	       )
	     )
   )
  )
  


(defn close-agent [k ^clojure.lang.Agent agnt]
  (dosync
   (send agnt (watch-critical-error close-roll-agent) ) 
   (alter fileMap (fn [p] (dissoc p k))) )
  )

(defn check-roll[^clojure.lang.IFn f-check]
  "Receives a function f-check parameter FileRS that should return true or false"
   (doseq [[k agnt]  @fileMap]
      (send agnt (watch-critical-error  (fn [frs] (when (not (nil? frs)) (if (f-check frs) (close-agent k agnt))  )  frs )))
      ))


(defn default-roll-check [{:keys [file output]} ]
    "Checks the file size and roll on size"
    (.flush output)
    (or 
      (>= (.length file) (get-conf2 :roll-size 10485760))
      (let [ file-last-modified (.lastModified file) 
             now-ms (System/currentTimeMillis) 
             roll-timeout (get-conf2 :roll-timeout 30000)
             diff-ms (- now-ms file-last-modified)
             ]
           (>= diff-ms roll-timeout)
        )
    )
  )

(defn start-services [] 
  
  ;start executor only if no null else return the same executor
  (dosync
    (alter file-resource-exec-service (fn [p] 
                          (if (nil? p) 
                             (let [service  (java.util.concurrent.Executors/newScheduledThreadPool 1)]
                               (.scheduleWithFixedDelay service #(check-roll default-roll-check) 1000 1000 java.util.concurrent.TimeUnit/MILLISECONDS)
                               service
                               )
                              p
                             )
                          )
           )
    )
  )

(defn close-all [] 
  (dosync (if @file-resource-exec-service
    (.shutdown @file-resource-exec-service)
   ))
   (doseq [[k agnt]  @fileMap]
      (close-agent k agnt)
      (await-for 10000 agnt)      
      )
   (info "Closed all file agents")  
)

