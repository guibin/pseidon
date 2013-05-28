(ns pseidon.test.fileresource_test)
(use '[midje.sweet])
(use '[pseidon.core.fileresource])

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
          (with-open [out (create-file fileName gzip-codec (org.apache.hadoop.io.compress.CodecPool/getCompressor gzip-codec) )])
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

(facts "Test file resource writing"
       (fact "Write to file"
           
             (write "test" "test-2013-05-28" (fn [output] (prn "output " output) ) )
             (close-all)
             (await-for 10000)
            
             1 => 1
              ;(prn  (filter #(re-find #".\.gz") (file-seq (clojure.java.io/file baseDir))))
             ;(> (count (filter #(re-find #".\.gz") (file-seq (clojure.java.io/file baseDir)))) 0 ) => true
             
       ))

