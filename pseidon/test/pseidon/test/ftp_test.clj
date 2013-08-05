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

(def conn (ftp-connect host uid pwd))


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
                   ]
                  (ftp-put conn local-file remote-file )
                  (let [files (get-files conn "test" "/a/b/testgetfiles" (fn [x] true) )]
                     (count files) => 1
                     (doseq [file files]
                        (doseq [line (get-line-seq! conn "test" file 10 :db test-db)]
                           (println "!!!! line " line)
                          )
                       )
                    )
                    
                )
          )
          
          (fact "Test recover messages in dis-order"
                (let [vec-pos-seq [ [0 10] [15 16]  [13 14] [11 12]  ]
                      counter (java.util.concurrent.atomic.AtomicInteger.)
                      local-file "resources/conf/log4j.properties" 
                      remote-file "/a/b/ctestinputstream/log4j.properties"
                      ]
                      (ftp-put conn local-file remote-file)
                      (recover conn remote-file vec-pos-seq (fn [rdr x n]
                                                         (.getAndIncrement counter)
                                                         )
                        )
                      (.get counter ) => 4
                  ))
       )
        
       (finally (.stop sshd-server))
       ))
    )