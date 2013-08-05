(ns pseidon.core.ds.ftp
  
  (:require   [pseidon.core.conf :refer [get-conf get-conf2] ]
              [pseidon.core.datastore :refer [inc-data! get-data-number] ]
              [pseidon.core.tracking :refer [mark-run!]]
  )
  (:use clojure.tools.logging
  ))

(import
   '(org.apache.commons.vfs2 FileContent FileObject FileSystemManager FileSystemOptions VFS Selectors)
   '(org.apache.commons.vfs2.auth StaticUserAuthenticator)
   '(org.apache.commons.vfs2.impl DefaultFileSystemConfigBuilder)
  )

(defn ftp-connect [^String host ^String uid ^String pwd]
  "save this connection to a global value.
   returns a map with :fs file-manager :host host-string :auth StatusUserAuthenticator :opts FileSystemOptions
  "
  (let [fs-manager (VFS/getManager) 
        auth (StaticUserAuthenticator. nil uid pwd)
        opts (FileSystemOptions.)
        ]
        (doto (DefaultFileSystemConfigBuilder/getInstance) (.setUserAuthenticator opts auth))
        {:fs fs-manager :host host :auth auth :opts opts}
  
        ))

(defn ftp-put [{:keys [fs host opts] } ^String file ^String dest] 
  "Performs a FTP/SFTP put"
  (let [url (clojure.string/join "/" [host dest]) ]
    (with-open [remote-fs-obj (-> fs (.resolveFile url opts)) ;remote file object 
               local-fs-obj  (-> fs (.resolveFile (-> (java.io.File. file) .getAbsolutePath ) )) ] ;local file object
     (-> remote-fs-obj (.copyFrom local-fs-obj (Selectors/SELECT_SELF)))
   )))


(defn ftp-get [{:keys [fs host opts] } ^String remote ^String local]
  "Performs a FTP/SFTP get"
  (let [url (clojure.string/join "/" [host remote]) local-file (java.io.File. local)]
    (with-open [
               remote-fs-obj (-> fs (.resolveFile url opts)) ;remote file object 
               local-fs-obj  (-> fs (.resolveFile (-> local-file .getAbsolutePath ) )) ] ;local file object
     (.mkdirs (.getParentFile local-file)) ;ensure that the parent directories exist
     (-> local-fs-obj (.copyFrom remote-fs-obj (Selectors/SELECT_SELF)))
   )))

(defn ftp-mv [{:keys [fs host opts] } ^String f1 ^String f2] 
  "Performs a FTP/SFTP move/rename"
  (let [url1 (clojure.string/join "/" [host f1]) url2 (clojure.string/join "/" [host f2]) ]
    (with-open [f1-obj (-> fs (.resolveFile url1 opts)) ;remote file object 
                f2-obj (-> fs (.resolveFile url2 opts))] ;local file object
     (-> f1-obj (.moveTo f2-obj))
   )))

(defn safe-resolve-file [fs url opts]
   (try (.resolveFile fs url opts) (catch Exception e (do (error (str e " " url " " opts) ) nil) ))
  )
(defn resolve-file [fs url opts]
   (let [f (loop [i 0 fs-obj nil]
     (if (and (< i 10) (nil? fs-obj))
           (recur (inc i) (safe-resolve-file fs url opts) )
           fs-obj
  ))]
   
     (if (nil? f) (throw (java.lang.Exception. (str "Cannot resolve file for " url ))) f)
   ))

(defn ftp-details [ {:keys [fs host opts] } ^String remote ]
  "Returns a map with :size on ftp server, :last-modified-time :attributes"
   (let [url (if (.startsWith remote "/") (str host remote) (clojure.string/join "/" [host remote])) ]
    (with-open [f-obj  (resolve-file fs url opts)] 
     (let [cnt (.getContent f-obj)]
       {:size (.getSize cnt) :last-modified-time (.getLastModifiedTime cnt) :attributes (.getAttributes cnt) }
   ))))

(defn ftp-inputstream [ {:keys [fs host opts] } ^String remote ]
  "Returns an inputstream for the file"
   (let [url (clojure.string/join "/" [host remote]) 
         f-obj (-> fs (.resolveFile url opts))
         in (-> f-obj .getContent .getInputStream)]
          (proxy [java.io.InputStream] []
				    (available [] (.available in))
				    (mark [limit] (.mark in))
				    (markSupported [] (.markSupported in))
				    (read  ([] (.read in))
                   ([bts] (.read in bts))
                   ([bts off len] (.read in bts off len))
                )
				    (reset [] (.reset in))
				    (skip [n] (.skip n in))
				    (close [] ;we need to close obht the f-obj and the inputstream
				      (.close in)
				      (.close f-obj)
				    )
				  )
         ))

 

(defn ftp-mkdirs [{:keys [fs host opts] :as m } ^String remote]
  "Performs a FTP/SFTP mkdir on all parent directories of remote before calling mkdir on the remote directories"
  (def _mkdir (fn [parent dir]
               (let [abs_dir (clojure.string/join "/" [parent dir])  url (clojure.string/join "/" [host abs_dir]) ]
                
                 (with-open [remote-fs-obj (-> fs (.resolveFile url opts))]
                    (if (not (.exists remote-fs-obj) ) 
                        (.createFolder remote-fs-obj)
                      ))
                  
                  (clojure.string/join "/" [parent dir])
                )))
                 
  
    ;we apply the mkdir function to each subdirectory
    (reduce _mkdir  (clojure.string/split remote #"/") )
    )

(defn ftp-exists? [{:keys [fs host opts] } ^String remote]
  "Performs a FTP/SFTP exists"
  (let [url (clojure.string/join "/" [host remote])]
    (with-open [
               remote-fs-obj (-> fs (.resolveFile url opts)) ;remote file object 
               ]
     (.exists remote-fs-obj)
   )))


(defn ftp-rm [{:keys [fs host opts] } ^String remote]
  "Performs a FTP/SFTP rmdir"
  (let [url (clojure.string/join "/" [host remote])]
    (with-open [
               remote-fs-obj (-> fs (.resolveFile url opts)) ;remote file object 
               ]
     (if (.exists remote-fs-obj) (.delete remote-fs-obj))
   )))

(defn ftp-ls [{:keys [fs host opts] } ^String remote]
  "Performs a FTP/SFTP list on the remote dir"
  (let [url (clojure.string/join "/" [host remote])]
    (with-open [remote-fs-obj (-> fs (.resolveFile url opts)) ]
      (map #(let [name (-> % .getName .getPathDecoded)] (.close %) name) (.getChildren remote-fs-obj))
   )))



(defn save-file-data [ns file total-char-count]
  "Increments the file data by (.length line)  the argument line can be a single line or a sequence of lines"
   (if (pos? total-char-count)   
     (inc-data! ns file  total-char-count)
     )
  )

(defn get-file-data [ns file]
  "Returns a map with :sent-size and :file"
  {:sent-size (get-data-number ns (str file)) :file file }
  )


(defn filter-done [{:keys [size sent-size] }]
  "Returns false if the size and sent-size are equal"
  (not= size sent-size
       ))


 (defn get-files [conn ns dir pred-filter]
   "Get only files that have not been sent yet
    the pred-filter is applied using filter
   "
 (let [files  (ftp-ls conn dir)
      names (map :file (filter filter-done (map #(conj (ftp-details conn %)  (get-file-data ns %) ) (filter pred-filter files)) ) )
      ]
    names
  ))
 
(defn ftp-record-id [ns file start-pos end-pos]
  (clojure.string/join \u0001 [ns file start-pos end-pos])
  )

(defn file-line-seq! [conn ns file reader pos line-buff & {:keys [db] }]
  "
    This methods does have side affects because it needs to keep track of which lines have been read already.
    pos is the starting point from where the file is read.
    For each line we return  [start_position end_position lines] ; the start and end position defines where in the file the lines were read from.
   
    For each batch of lines read the mark-run! function is called with (mark-run! ns (clojure.string/join \u0001 [file start-pos end-pos]))
    The id for each record is file\u0001start_position\u0001end_position, this function will use the named convention to save records to the tracking service.
   

  "
     (def lf 0xA)
     (def cr 0xD)

		(defn n-read-line [rdr]
		  (loop [buff (java.lang.StringBuilder.)
		         ch (.read rdr)
		         total 0
		         ]
		    
		    (if (or (= ch -1) (= ch lf))
		      (let [ s (.toString buff) ]
		        (if (pos? (.length s) )
		          [s (inc total)] ;we inc for the new line char
		          [nil 0]
		          )
		        )
		      (do
		        
		        (if (complement (= ch cr)) 
		          (.append buff (char ch))
		          )
		        
		        (recur buff (.read rdr) (inc total))
		        
		        )
		      )
		    )
		  )

     (defn safe-add-lines [line lines]
       (if (nil? lines) [line] (conj lines line)))
     
     (defn read-lines [n]
       (loop [i n lines nil total-char-count 0]
         (let [[line char-count] (try (n-read-line reader) (catch Exception e [nil 0]) )]
           (if (nil? line) (.close reader))
           (let [chars (+ total-char-count char-count) ]
	           
             (if (nil? line)
	                [lines chars]
                 (if (zero? i)
                   [(safe-add-lines line lines) chars] ;we must add the last line
	                (recur (dec i) (safe-add-lines line lines)  chars) ;get more lines   
                   )
                 )
             )
           )
         )
       )
      
     (defn read-lines-save-data [start-pos n]
       "
        Returns [start-pos end-pos lines]
       "
       (let [[lines total-char-count] (read-lines n) ]
       (if (not (nil? lines))
         (let [end-pos (+ start-pos total-char-count)]
           (mark-run! ns (ftp-record-id ns file start-pos end-pos) :db db) ;mark in the tracing api
                                                                 ;event if the zookeeper pointer save fails the recovery service now has the information to recover these lines if needed.
           (save-file-data ns file total-char-count) ; we save the pointer to zookeeper
           [start-pos end-pos lines] ; returns [start end lines]
           )
         )
       )
       )
      
     
     (defn read-batched [start-pos]
       " 
         Returns [start-pos end-pos lines]
       "
       (let [ [start-pos2 end-pos2 lines :as record] (read-lines-save-data start-pos line-buff) ]
         (if (not (nil? lines))
            (do
              (lazy-seq (cons record (read-batched end-pos2)) )
              )
           )
         )
       )
     
       (read-batched pos)
          
       )
  
(defn get-line-seq! [conn ns file line-buff & {:keys [db]}]
  "Helper method for ftp data sources, returns a reader that will save the number of characters read on each readLine call
   The method will also read the file data and skip the characters already read.
   Items in the sequence have format [start-pos end-pos lines]
  "
    (let [pos (:sent-size (get-file-data ns file) )
          reader  (-> (ftp-inputstream conn file) java.io.InputStreamReader. java.io.BufferedReader.)]
          (if (pos? pos) (pseidon.util.Bytes/skip reader pos)) ;skip n characters
          (file-line-seq! conn ns file reader pos line-buff :db db)
          
          )
    )


(defn recover [conn file pos-vect-seq f-send]
  "For every position vector (start, stop) the function f-send accepts arguments [reader start records-to-read]"
		  (letfn [(get-rdr [] (-> (ftp-inputstream conn file) java.io.InputStreamReader. java.io.BufferedReader.) )
              
              (send-file-data [reader start max]
                               (f-send reader start max))
              
              (send-file [[reader prev-x prev-n] [x n]]
                         (let [diff (- x (+ prev-x prev-n))
                                   
                                   rdr (cond (> 1 diff) ;skip the gap and return the same reader
			                                 (do 
                                          (pseidon.util.Bytes/skip reader diff) 
                                          reader
                                          )
			                                 (< 0 diff)
			                                 (do (.close reader)  ;close the reader and reopen a new one
                                           (let [reader2 (get-rdr)] 
                                             (pseidon.util.Bytes/skip reader2 (- x 1) )
                                             reader2
                                             )
                                           )
			                                 :else reader
			                                 )
                                 ]
                               (send-file-data reader x n)
                               [rdr x n]
                               ))]
         
       (let [[reader x n] (reduce send-file [(get-rdr) 0 0] (sort-by first pos-vect-seq))]
          (.close reader) ;close the last reader
         )
       )
    )
