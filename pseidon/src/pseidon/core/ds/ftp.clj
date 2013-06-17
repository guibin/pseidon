(ns pseidon.core.ds.ftp
  
  (:require   [pseidon.core.conf :refer [get-conf get-conf2] ]
              
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

(defn ftp-details [ {:keys [fs host opts] } ^String remote ]
  "Returns a map with :size on ftp server, :last-modified-time :attributes"
   (let [url (clojure.string/join "/" [host remote]) ]
    (with-open [f-obj (-> fs (.resolveFile url opts))] 
     (let [cnt (.getContent f-obj)]
       {:size (.getSize cnt) :last-modified-time (.getLastModifiedTime cnt) :attributes (.getAttributes cnt) }
   ))))

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




