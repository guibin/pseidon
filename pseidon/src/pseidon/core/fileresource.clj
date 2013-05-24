(ns pseidon.core.fileresource
   (:use clojure.tools.logging)
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

;alter the fileMap to contain the  agent
(defn add-agent [topic key]
  (prn "Adding agent " (keys @fileMap))
  (let [codec (get-codec topic) 
        compressor (org.apache.hadoop.io.compress.CodecPool/getCompressor codec)
        agnt (agent (->FileRS (create-file-name (clojure.string/join "/" [baseDir key]) codec) nil codec compressor ) )]
        (alter fileMap (fn [p] (assoc p (keyword key) agnt )))
        agnt
  ))

;get an agent and if it doesnt exist create one with a FileRS instance as value
(defn get-agent [topic key]
  (dosync (if-let [agnt ((keyword key) @fileMap) ] agnt (add-agent topic key) )
  ))

;create a file using the codec
(defn create-file [name codec compressor]
     (let [ file (java.io.File. name)]
       (if-let [parent (.getParentFile file)] (.mkdirs parent) (prn "File has no parent " file) )
       (.createNewFile file)
       (if (.exists file) (info "Created " file) (throw (java.io.IOException. (str "Failed to create " file)))  )
       (.createOutputStream codec (java.io.BufferedOutputStream. (java.io.FileOutputStream. name ) ) compressor) 
     
     ))

(defn write-to-frs [frs writer]
  (let [codec (:codec frs) 
        frs-t (if (:output frs) frs (->FileRS name (create-file (:name frs) codec (:compressor frs) ) codec (:compressor frs) ))
        ]
      (writer (:output frs-t))
      ;we always return a FileRS instance
      frs-t))

; will do an async function send to that will call the writer (writer output-stream)
(defn write [topic key writer]
   
    (send-off (get-agent topic key) write-to-frs writer) 
    
  )

(defn close-roll-agent [frs]
  (.close (:output frs) ) 
  (org.apache.hadoop.io.compress.CodecPool/returnCompressor (:compressor frs) ) 
  )

(defn close-all [] 
  (doseq [[k agnt]  @fileMap]
      (send agnt close-roll-agent ) 
      (dosync (alter fileMap (fn [p] (dissoc p k))) )
      ))
