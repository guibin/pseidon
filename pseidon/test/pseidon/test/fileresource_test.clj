(ns pseidon.test.fileresource_test)
(use '[midje.sweet])
(use '[pseidon.core.fileresource])

(facts "Test internals of the file resource"
       (fact "get codec test"
             
             (class (get-codec "blabla")) => org.apache.hadoop.io.compress.GzipCodec 
       
       ))