(ns pseidon.core.fileresource
   (:use clojure.tools.logging 
         pseidon.core.watchdog)
  )

(defrecord FileRS [name output codec compressor])
(defrecord TopicConf [key codec])

(def baseDir "target")

(def fileMap (ref {}))
(def codecMap {})

(def gzip-codec (org.apache.hadoop.io.compress.GzipCodec.))

;find codec from codec map otherwise return the GzipCodec
(defn get-codec [topic]
   (if-let [codec ((keyword topic) codecMap)] codec gzip-codec)
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
  
     (clojure.string/join "." (assoc s (- s-count 1) (last-nth s 2) (- s-count 2) (last-nth s 1) ) )
     
  ))


;get an agent and if it doesnt exist create one with a FileRS instance as value
(defn get-agent [topic key]
 ;alter the fileMap to contain the  agent
 (defn add-agent [topic key]
  (prn "Adding agent " (keys @fileMap) " topic " topic " key  " key)
  (let [codec (get-codec topic) 
        compressor (org.apache.hadoop.io.compress.CodecPool/getCompressor codec)
        agnt (agent (->FileRS (create-file-name (clojure.string/join "/" [baseDir key]) codec) nil codec compressor ) )]
        (set-error-handler! agnt agent-error-handler)
        (alter fileMap (fn [p] (assoc p key agnt )))
        agnt
  ))

  (dosync (if-let [agnt (get @fileMap key) ]  agnt (add-agent topic key) )
  ))

;create a file using the codec
(defn create-file [^String name ^org.apache.hadoop.io.compress.CompressionCodec codec ^org.apache.hadoop.io.compress.Compressor compressor]
     (let [ file (java.io.File. name)]
       (if-let [parent (.getParentFile file)] (.mkdirs parent) (info "File has no parent " file) )
       (.createNewFile file)
       (if (.exists file) (info "Created " file) (throw (java.io.IOException. (str "Failed to create " file)))  )
       (.createOutputStream codec (java.io.BufferedOutputStream. (java.io.FileOutputStream. name ) ) compressor) 
     
     ))

(defn write-to-frs [^FileRS frs ^clojure.lang.IFn writer]
  (let [codec (:codec frs) 
        frs-t (if (:output frs) frs (->FileRS (:name frs) (create-file (:name frs) codec (:compressor frs) ) codec (:compressor frs) ))
        ]
      (writer (:output frs-t))
      ;we always return a FileRS instance
      frs-t))

; will do an async function send to that will call the writer (writer output-stream)
(defn write [topic key ^clojure.lang.IFn writer]
   (dosync (send-off (get-agent topic key) write-to-frs writer) )
  )

(defn close-roll-agent [^FileRS frs]
  (if (and (not (nil? frs)) (not (nil? (:output frs) ) ) ) 
  (do
  (.close (:output frs) ) 
  (if (nil? (:compressor frs)) (org.apache.hadoop.io.compress.CodecPool/returnCompressor (:compressor frs) ))
  ;find rename the file by removing the last \.([a-zA-Z0-9])+_([0-9]+) piece of the filename and appending (group2).(group1)
  (let [file (java.io.File.  (:name frs)  ) 
        new-file  (java.io.File. (create-rolled-file-name (:name frs) ))
        renamed (.renameTo file new-file)
        ]
     (if renamed (info "File " new-file " created") (throw Exception "Unable to roll file from " file " to " new-file) ) 
  ))))


(defn close-agent [k ^clojure.lang.Agent agnt]
  (dosync
   (send agnt (watch-agent-error close-roll-agent) ) 
   (alter fileMap (fn [p] (dissoc p k))) )
  )

(defn check-roll[^clojure.lang.IFn f-check]
  "Receives a function f-check parameter FileRS that should return true or false"
   (doseq [[k agnt]  @fileMap]
     (send agnt watch-agent-error  (fn [frs] (if (f-check frs) (close-agent k frs) ) ))
      ))

(defn close-all [] 
  (doseq [[k agnt]  @fileMap]
      (close-agent k agnt)
      ))


