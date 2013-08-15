(ns pseidon.test.ftp_test
  
  (:use midje.sweet
        pseidon.core.conf
        pseidon.core.ds.ftp
        pseidon.core.tracking
         ))

(import '(org.apache.sshd SshServer)
        '(org.apache.sshd.server Command CommandFactory)
        '(org.apache.sshd.server.command ScpCommandFactory)
        '(org.apache.sshd.server.auth UserAuthNone$Factory)
        '(org.apache.sshd.server.keyprovider SimpleGeneratorHostKeyProvider)
        '(org.apache.sshd.server.sftp SftpSubsystem$Factory)
        '(org.apache.sshd.server.shell ProcessShellFactory)
        '(org.apache.commons.io FileUtils)
        )
 
(def test-db (create-spec (str "target/fpt_test/" (System/currentTimeMillis)) ))

(def uid "test")
(def pwd "test")
(def port 7117)
(def host (str "sftp://localhost:" port))

(defn setup[]
  (let [sshd (SshServer/setUpDefaultServer) key-pair-provider (SimpleGeneratorHostKeyProvider. "hostkey.ser")]
     (doto sshd (.setPort port) 
                (.setKeyPairProvider key-pair-provider)
                (.setUserAuthFactories [(UserAuthNone$Factory.)] )
                (.start)
                (.setSubsystemFactories [(SftpSubsystem$Factory.)])
                (.setCommandFactory (ScpCommandFactory. (reify CommandFactory
                                      (createCommand [this cmd]
                                       (-> (ProcessShellFactory. (clojure.string/split cmd #" ")) .create )
                                      ))) )
        )
    
    ))

(defn get-dbspec [p]
  (let [p1 (str p "/" (System/currentTimeMillis))
        file (java.io.File. p1)]
    (.mkdirs file)
    (create-spec (str p1 "/mydb"))
    ))

(def conn (ftp-connect host uid pwd))

(facts "Test Util mehods"
       (fact "Test structure and destructure ftp id"
 ;(defn ftp-record-id [ns file start-pos end-pos]
  
             (let [ns "abc" file "123" start-pos 1 end-pos 100
                   [ns2 file2 start-pos2 end-pos2] (destruct-ftp-record-id 
                                                     (ftp-record-id ns file start-pos end-pos))
                   ]
               ns => ns2
               file => file2
               start-pos => start-pos2
               end-pos => end-pos2
               )
             )
       )

(facts "Test ftp list get and put"
    (let [sshd-server (setup)
          zk-server (org.apache.curator.test.TestingServer.)
          zk-url (str "localhost:" (.getPort zk-server))
          ]
       
       (load-default-config!)
       (set-conf! :zk-url zk-url)

       (try
        (do
       (fact "Test put/get files"
             ;put multiple files
             (let [local-file "resources/conf/logging.clj" 
                   remote-file "/a/b/c/logging.clj"
                   local-file2 "target/test-ftp-get-set.clj"
                   ]
             (ftp-put conn local-file remote-file )
             (ftp-get conn remote-file local-file2)
             (.exists (java.io.File. local-file2) ) => true
             (FileUtils/contentEquals (java.io.File. local-file) (java.io.File. local-file2)) => true
             ))
       
       (fact "Test ftp details"
             ;put multiple files
             (let [local-file "resources/conf/logging.clj" 
                   remote-file "/a/b/c/logging.clj"
                   ]
             (ftp-put conn local-file remote-file )
             (let [details (ftp-details conn remote-file) ]
                (:size details) => 195                                          
             )))
       
       (fact "Test delete mkdir and exist"
             (let [dir "/a/b/myremotedir"]
             (ftp-mkdirs conn dir)
             (ftp-exists? conn dir) => true
             (ftp-rm conn dir)
             (ftp-exists? conn dir) => false
             ))
       (fact "Test delete mkdir and exist"
             (let [dir "/a/b/ftpls" local-file "resources/conf/logging.clj"]
             (ftp-rm conn dir)
             (ftp-mkdirs conn dir)
             
             (doseq [i [1 2 3]] (ftp-put conn local-file (str dir "/" i) ))
             (let [files (ftp-ls conn dir) ]
               (count files) => 3
             )))
       
       (fact "Test move files"
             (let [local-file "resources/conf/logging.clj" 
                   f1 "/a/b/c1/logging.clj"
                   f2 "/a/b/c1/movedfile"
                   ]
             (ftp-put conn local-file f1)
             (ftp-mv conn f1 f2)
             (ftp-exists? conn f2) => true
             ))
        (fact "Test ftp-inputstream"
             (let [local-file "resources/conf/log4j.properties" 
                   remote-file "/a/b/ctestinputstream/log4j.properties"
                   test-file "target/testftpinputstream"
                   ]
             (ftp-put conn local-file remote-file )
             (with-open [in (-> (ftp-inputstream conn remote-file) java.io.InputStreamReader. java.io.BufferedReader.) 
                         out (-> (java.io.File. test-file) java.io.PrintWriter.)]
                (doseq [line (line-seq in)]
                  (.println out line)
             ))
             
             (FileUtils/contentEquals (java.io.File. local-file) (java.io.File. test-file)) => true
             ))
        
          (fact "Test get-files and get-reader"
                (let [local-file "resources/conf/log4j.properties" 
                   remote-file "/a/b/testgetfiles/log4j.properties"
                   db (create-spec "target/test-getfiles123")
                   ]
                  (ftp-put conn local-file remote-file )
                  (let [files (get-files conn "test" "/a/b/testgetfiles" (fn [x] true) :db db)]
                     (count files) => 1
                     (doseq [file files]
                        (doseq [line (get-line-seq! conn "test" file 10 :db test-db)]
                           (println "!!!! line " line)
                          )
                       )
                    )
                    
                )
          )
          
          (fact "Test List files with tracked files"
                
                (let [local-file "resources/conf/log4j.properties" 
                   remote-file "/listtrackers/testgetfiles/log4j.properties"
                   remote-file2 "/listtrackers/testgetfiles/log4j2.properties"
                   db (get-dbspec "target/mytesttrackedfilesdb/mydb")
                   ]
                  (ftp-put conn local-file remote-file )
                  ;mark file as ready
                  ;(defn mark-run! [^String ds ^String id & {:keys [db]}]
                  (mark-run! "test" (ftp-record-id "test" remote-file2 0 10) :db db)
                  (let [files (get-files conn "test" "/listtrackers/testgetfiles" (fn [x] true) :db db)]
                     (count files) => 2
                     (sort files) => [remote-file remote-file2]
                     )))
          (fact "Test Recover messages from tracked files"
                
                (let [local-file "resources/conf/log4j.properties" 
                   remote-files  #{"/listtrackers/testgetfiles/log4j.properties" 
                                  "/listtrackers/testgetfiles/log4j2.properties"
                                  "/listtrackers/testgetfiles/log4j3.properties"}
                   
                   db (get-dbspec "target/mytesttrackedfilesdb2/mydb")
                   ]
                  (doseq [file remote-files]
                    (mark-run! "test" (ftp-record-id "test" file 0 10) :db db)
                    (mark-run! "test" (ftp-record-id "test" file 11 20) :db db)
                    )
                  (let [recover-message-map (load-recover-messages! "test" :db db)]
                    (doseq [[k msg-seq] recover-message-map]
                      (count msg-seq) => 2
                      (contains? remote-files k) => true
                      )
                    )))
          
          (fact "Test recover message read files"
                ;recover-line-seq
                (let [local-file "resources/conf/log4j.properties" 
                      file1 "/listtrackers2/testgetfiles/log4j.properties"
                      remote-files  #{file1}
                      db (get-dbspec "target/mytesttrackedfilesdb3/mydb")
                   ]
                  (ftp-put conn local-file file1 )
                  (doseq [file remote-files]
                    (mark-run! "test" (ftp-record-id "test" file 0 27) :db db)
                    (mark-run! "test" (ftp-record-id "test" file 27 (+ 27 38)) :db db)
                    
                    )
                 
                  (let [recover-message-map (load-recover-messages! "test" :db db)
                       
                        ]
                     (doseq [[file v] recover-message-map]
                       (let [
                             recover-seq (recover-line-seq conn file (map pos-vec-extract v))
                             ]
                          (first recover-seq) => [0, 27 ["# Just one of those things"]]
                          (second recover-seq) => [27, (+ 27 38) ["datestamp=yyyy-MM-dd/HH:mm:ss.SSS/zzz"]]
                         ))
                         
                         
                                          
                    )
                ))
          (fact "Test deleting done files"
                (let [local-file "resources/conf/log4j.properties" 
                   remote-file1 "/a/b/testdeletingdonefiles/log4j-1.properties"
                   remote-file2 "/a/b/testdeletingdonefiles/log4j-2.properties"
                   remote-file3 "/a/b/testdeletingdonefiles/log4j-3.properties"
                   db (create-spec "target/test-deletedonefiles123")
                   ns (str "test-" (System/currentTimeMillis))
                   slurp-file (fn [file]
                                ;reads in the whole file and marks every message as done
                                (with-txn db
                                (dorun
                                  (let [[[x y lines]] (get-line-seq! conn ns file 100 :db db)]
                                    (mark-done! ns (ftp-record-id ns file x y) #() :db db)
                                    ))))
                                
                   ]
                  (ftp-put conn local-file remote-file1 )
                  (ftp-put conn local-file remote-file2 )
                  (ftp-put conn local-file remote-file3 )
                  
                  ;consume file1
                  (slurp-file remote-file1)
                  
                  ; delete-done-file [conn ns file-name & {:keys [db] :or {db dbspec}} ]
                  (delete-done-file conn ns remote-file1 :db db) => true
                  (delete-done-file conn ns remote-file2 :db db) => false
                  (delete-done-file conn ns remote-file3 :db db) => false
                  
                  (ftp-exists? conn remote-file1) => false
                  (ftp-exists? conn remote-file2) => true
                  (ftp-exists? conn remote-file3) => true
                  
                
                ))
                 
       )
        
       (finally (.stop sshd-server))
       ))
    )