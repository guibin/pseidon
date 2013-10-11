(ns pseidon.kafka.test.producer-tests
  (:import [kafka.message MessageAndMetadata MessageAndOffset]
           [kafka.producer KeyedMessage])
  (:require [pseidon.core.conf :refer [get-conf set-conf!]])
  (:use midje.sweet
        pseidon.kafka.producer
        ))

(facts "Test utils"
       
      
       (fact "to-bytes"
             (vec (toBytes "hi")) => (vec (.getBytes "hi" "UTF-8"))
             (vec (toBytes (.getBytes "hi" "UTF-8"))) => (vec (.getBytes "hi" "UTF-8"))
            
            ))
                           
