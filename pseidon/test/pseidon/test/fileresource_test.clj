(ns pseidon.test.fileresource_test)
(use '[midje.sweet])
(use '[pseidon.core.fileresource])
(use '[pseidon.core.conf])


;(def compressor-pool-factory (CompressionPoolFactoryImpl. 100 100 nil))

(facts "Test internals of the file resource"
       (fact "get default codec test"
             
             (class (get-codec "blabla")) => org.apache.hadoop.io.compress.GzipCodec 
       
       )
       
       (fact "Test create file name"
        ;(create-file-name "mykey" (get-codec "blabla")) => "mykey.gz_"
             
       )
       (fact "Add agent"
             (let [topic "mytopic" key "mykey" agnt (get-agent topic key) ]
              (class agnt) => clojure.lang.Agent
             ))
       (fact "Create File"
         (let [fileName "target/testdir/mytestfile.txt"]
           ;open and close the file
          (with-open [out (create-file (java.io.File. fileName) gzip-codec (-> compressor-pool-factory (.get gzip-codec) ) )])
          (.exists (java.io.File. fileName)) => true
          (clojure.java.io/delete-file fileName)
          
       ))
       
       (fact "Roll file name creation"
         (let [filename "myfile-123.gz_121212"
               expected "myfile-123.121212.gz"
               ]
         (create-rolled-file-name filename ) => expected
       ))
)

(defn delete-test-files []
  (doseq [f (filter #(-> %1 .getName (.endsWith ".gz")  ) (file-seq (clojure.java.io/file (get-writer-basedir))))] (.delete f))
  )

(facts "Test file resource writing"
       (fact "Write to file"
             ;remove files
             (delete-test-files)
             (write "test" "test-2013-05-28" (fn [output] (prn "output " output) ) )
             (prn "Calling close-all method " close-all)
             (close-all)
             (await-for 10000) => true
             (Thread/sleep 1000)
             
             (>  (count (filter #(-> %1 .getName (.endsWith ".gz")  ) (file-seq (clojure.java.io/file (get-writer-basedir) )))) 0) => true
             
       ))

