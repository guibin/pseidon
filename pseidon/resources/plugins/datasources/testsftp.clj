(ns plugins.datasources.testsftp
  (:require
      [pseidon.core.ds.ftp :refer [ftp-connect ftp-details ftp-ls ftp-inputstream]]
      [pseidon.core.datastore :refer [get-data-long]]
      [pseidon.core.conf :refer [get-conf2]]
    )
    (:use pseidon.core.registry)
  )


;(defrecord DataSource [name start stop list-files reader])

(def ^:dynamic url (get-conf2 "testftp-url" "sftp://192.168.56.101"))
(def ^:dynamic uid (get-conf2 "testftp-uid" "ftptest"))
(def ^:dynamic pwd (get-conf2 "testftp-pwd" "tech!sw78"))

(def ^:dynamic conn (ftp-connect url uid pwd))
(def ^:dynamic name-space "testftp")

(defn get-file-data [file]
  (comment
  "Returns a map with :sent-size and :file"
  {:sent-size (get-data-long name-space (str file)) :file file }
  )
  {:sent-size 0 :file file}
  )

(defn filter-done [{:keys [size sent-size] }]
  "Returns false if the size and sent-size are equal"
  (not (= size sent-size)
       ))

 (defn get-files []
 (let [files  (ftp-ls conn "/") ]
     (map :file (filter filter-done (map #(conj (ftp-details conn %)  (get-file-data %) ) (filter #(.endsWith % ".txt") files)) ) )
  ))

(defn get-reader [file]
  (doto (ftp-inputstream file) java.io.InputStreamReader. java.io.BufferedReader.)
  )

;register the testftp datasource
(register (->DataSource "testftp" #() #(.close conn) get-files get-reader))
             


 


