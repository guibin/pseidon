(ns pseidon.hdfs.cloudera.core.hdfsprocessor
  
  (:require [pseidon.core.tracking :refer [mark-done!]]
            [pseidon.core.conf :refer [get-conf2]]
            [clj-time.coerce :refer [from-long]]
            [clj-time.core :refer [year month day hour]]
            [clj-time.format :refer [unparse parse formatter]]
            [clojure.core.async :refer [>! <! go chan]] 
            [pseidon.core.message :refer [get-ids]]
            [pseidon.core.registry :refer [register ->Processor]]
            [clojure.tools.logging :refer [info error]]
           )
           
   (:import (org.apache.commons.lang StringUtils)
           (java.io File)
           (org.apache.hadoop.fs Path FileUtil FileSystem)
           (org.apache.hadoop.conf Configuration)
           )
  )

;applys a function in the map depending on a model key
(defmacro apply-model [model-key model-map & args]
   (if-let [f# (get model-map model-key)]
   `(~f# ~@args)))

(defn ^:dynamic number-format [^Integer n]
  (if (< n 10) (str "0" n) (str n)))
  
(def ^:dynamic dt-formatter (formatter "yyyyMMdd"))
(def ^:dynamic hr-formatter (formatter "yyyyMMddHH"))

(def ^:dynamic hdfs-dir-formatters {1
                                    (fn [date-hr]
                                      "Expects the format yyyyMMddHH
                                       returns dt=yyyyMMdd/hr=yyyyMMddHH
                                       "
                                      (let [date (parse hr-formatter date-hr)
	                                          dt (unparse dt-formatter date)] ;parse dt
                                        (str "dt=" dt "/hr=" date-hr)))
                                    2 (fn [date-hr]
                                        "Expects the format yyyyMMddHH
                                         returns year=yyyy/month=MM/day=dd/hour=HH
                                         "
                                        (let [date (parse hr-formatter date-hr)]
                                          (str "year=" (year date) "/month=" (number-format (month date))
                                                                   "/day=" (number-format (day date))
                                                                   "/hour=" (number-format (hour date)))))
                                    })

(def ^:dynamic file-name-parsers {1 
                                     (fn [file-name]
                                       "Expects a file name with type_id_hr_yyyyMMddHH.extension
																			   use this method as (let [ [type id _ date] (parse-file-name file-name)]  ) 
																			  "
																			  (let [[type id _ date] (clojure.string/split file-name #"[_\.]")]
																			  (prn "Parsing file " file-name " to [ " type " " id " " date "]")

																			  [type id nil date]))
                                     2 (fn [file-name]
                                        "Expects a file name with type_yyyMMddHH.extension
                                         the id value part is returned empty
                                         " 
                                         (let [ [type date] (clojure.string/split file-name #"[_\.]")]
                                           [type "" date]))
                                     })

(defonce c (chan))

(def hdfs-url (get-conf2 "hdfs-url" "hdfs://localhost:8020"))

(defonce hdfs-conf (doto (Configuration.) (.set "fs.default.name" hdfs-url) ))
(defonce fs (ref nil))


(defn ^:dynamic get-hdfs-conf []
  hdfs-conf)

(defn ^:dynamic ^FileSystem get-fs []
  (dosync
    (if (nil? @fs)
      (let [fs2  ;CREATE File system here
                  (FileSystem/get (get-hdfs-conf)) 
                 ]
        (ref-set fs fs2)
        )))
  @fs
  )

(defn ^:dynamic file->hdfs [^String ds ^String id ^String local-file ^String remote-file]
  "
   Copy(s) a local file to hdfs,
   any exception is printed and false is returned otherwise true is returned
  "
  (try
    (do ;copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) 
      (info "Copy " local-file " to hdfs " remote-file " file " hdfs-url)
      (.copyFromLocalFile (get-fs) false (Path. local-file) (Path. remote-file))
      (info "Copy Done [local-file: " local-file "] [ds: " ds "] [id: " id "]")
      (mark-done! ds id (fn [] 
                          ;on mark done we delete the local file being copied.
                          (clojure.java.io/delete-file local-file true)
                          (info "deleted local-file " local-file)
                          ) )
      true
      )
    (catch Exception e (do 
                         (error e)
                         false
                         ))))


(defn ^:dynamic load-processor []
  (let [ ;globals
        local-file-model (get-conf2 "hdfs-local-file-model" 1) ;what local-file-model to apply
        hdfs-dir-model (get-conf2 "hdfs-dir-model" 1) ;what hdfs directory model to apply
        hdfs-dir (get-conf2 "hdfs-base-dir" "/log/raw") ;get the base hdfs dir 
        ] 
    
	  (letfn [
      (recover-ready-messages []
                              
                              
                              )
            
	    (copy! []
	           (go
	             (while true
	               (let [[ds id local-file remote-file]  (<! c)]
	                 ;retry till the file has been uploaded
	                 (while (false? (file->hdfs ds id local-file remote-file))
	                   (Thread/sleep 1000))))))             
	                   
	    (start [] 
                (recover-ready-messages)
                (copy!))
	    (stop [] (.close (get-fs))
	          )
	    (exec [ {:keys [topic ts ds] :as msg } ] ;unpack message
	          (let [
	                ids (get-ids msg)  ;get the message ids as a sequence
	                ]
	               (doseq [id ids
	                       ] ;for each id (the id should be the local file-name) upload the file
	                 (let [
	                       ^String local-file id
	                       ^String file-name (-> local-file java.io.File. .getName)
	                       [type-name id _ date-hr] (apply-model local-file-model file-name-parsers file-name); inserts the correct model function
	                       date-dir (apply-model hdfs-dir-model hdfs-dir-formatters date-hr)
	                       remote-file (str hdfs-dir "/" type-name "/" date-dir "/" file-name) ;construct the remote file-name
	                       ]
	                   (go 
	                     (>! c [ds id local-file remote-file]) ;here id is the local file
	                     )))))
	   
	    
	    ]
	    (register (->Processor "hdfs" start stop exec)))))


(load-processor)





