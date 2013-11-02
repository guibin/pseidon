(ns pseidon.test.core.chronicle-test
  (:require [pseidon.test.core.utils :refer [create-tmp-dir]]
            [pseidon.core.chronicle :refer [offer close create-queue]])
  (:use midje.sweet
        pseidon.core.chronicle))


(facts "Test chronicle queue implementation"
       
       (fact "Test offer"
             
             (let [limit 100000
                   path (create-tmp-dir "chronicle" :delete-on-exit false)
                   q (create-queue path limit :segment-limit 100)] 
               (doall
                 (dotimes [i limit]
                   (offer q (.getBytes (str "msg " i)))))
               (close q)
               true => true
               )
             
             ))

