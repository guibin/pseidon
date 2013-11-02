(ns pseidon.test.core.chronicle-test
  (:require [pseidon.test.core.utils :refer [create-tmp-dir]]
            [pseidon.core.chronicle :refer [offer poll poll! offer! close create-queue]])
  (:use midje.sweet
        pseidon.core.chronicle))


(facts "Test chronicle queue implementation"
      
      (fact "Test offer and get no limit or segment overflow"
             
             (let [limit 10000
                   path (create-tmp-dir "chronicle" :delete-on-exit true)
                   q (create-queue path limit :segment-limit (* limit 2))] 
               (doall
                 (dotimes [i 100]
                   (dotimes [n 100]
                     (offer! q (.getBytes (str "msg " i "-" n))))))
                 
               (close q)
                (let [read-queue (create-queue path limit)]
                  (dotimes [i 100]
                    (dotimes [n 100]
                      (String. (poll! read-queue)) => (str "msg " i "-" n)))
                  (close read-queue)))
             
             )
       (fact "Test offer write block on limit "
             (let [limit 10
                   path (create-tmp-dir "chronicle" :delete-on-exit true)
                   q (create-queue path limit)]
               (doall
                 (dotimes [i 11]
                   (offer! q (.getBytes (str "msg-" i)))))
               ;the following should block,timeout and return false
               (offer q (.getBytes (str "abc")) 100) => false 
               
             ))
       
         (fact "Test offer write block on limit read the recover from limit block again"
             (let [limit 10
                   path (create-tmp-dir "chronicle" :delete-on-exit true)
                   q (create-queue path limit)]
               (doall
                 (dotimes [i 11]
                   (offer! q (.getBytes (str "msg-" i)))))
               ;the following should block,timeout and return false
               (offer q (.getBytes (str "abc")) 100) => false 
               ;read two items
               (doall
                 (dotimes [i 3]
                   (not (nil? (poll q 100))) => true))
               
               ;write twice no blocking
               (doall
                 (dotimes [i 2]
                   (prn "write " i)
                   (offer q (.getBytes "msg") 100) => true))
               
               ;block again
               (offer q (.getBytes (str "abc")) 100) => false 
             ))

       )

