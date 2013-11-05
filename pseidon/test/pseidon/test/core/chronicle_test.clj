(ns pseidon.test.core.chronicle-test
  (:require [pseidon.test.core.utils :refer [create-tmp-dir]]
            [pseidon.core.chronicle :refer [offer poll poll! offer! close create-queue]])
  (:use midje.sweet
        pseidon.core.chronicle)
  (:import [java.io File]))


(facts "Test chronicle queue implementation"
       (comment
       (fact "Test directory discovery return nil if no subdirs"
             (let [path (create-tmp-dir "chronicle" :delete-on-exit true)] 
               (load-chronicle-path path) => nil))
       
       (fact "Test directory discovery return latest dir"
             (let [
                   path (create-tmp-dir "chronicle" :delete-on-exit true)
                   path2 (File. path "b")
                   path3 (File. path "a");names are reverse order to ensure ordering is by ts.
                   ]
               (.mkdirs path2)
               (Thread/sleep 1000)
               (.mkdirs path3)
               (load-chronicle-path path) => path3))
       
      
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
      )
       (fact "Test write close, open write again"
             
             (let [limit 100000
                   path (create-tmp-dir "chronicle" :delete-on-exit true)
                   write-close (fn []
                                 (let [q (create-queue path limit :segment-limit (* limit 2))]
                                   (doall
											                 (dotimes [i 100]
                                               (offer! q (.getBytes (str "msg " i)))))
	               
                                   (close q)))
                  ]
               
                  (dotimes [i 10]
                    (write-close))))
       
          (fact "Test offer write block on limit "
             (let [limit 10
                   path (create-tmp-dir "chronicle" :delete-on-exit true)
                   q (create-queue path limit)]
               (doall
                 (dotimes [i 11]
                   (offer q (.getBytes (str "msg-" i)) 100)))
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
                   (offer q (.getBytes "msg") 100) => true))
               
               ;block again
               (offer q (.getBytes (str "abc")) 100) => false 
             ))
         
          (fact "Test offer, roll on segment 10 times, read verify data"
             (let [limit 10
                   path (create-tmp-dir "chronicle" :delete-on-exit true)
                   q (create-queue path limit :segment-limit 20)
                   write-read (fn [n] [(doall
                                         (for [i (range n)]
                                           (let [msg (str "msg-" i)]
                                             (offer q (.getBytes msg) 100)
                                             msg
                                           )))
                                       
                                       (doall
                                         (map #(String. %) (filter (complement nil?) (repeatedly n #(poll q 100) ) ))
                                         )])
                                
                                       
                   ]
                   
                   (doall
                     (dotimes [i 20]
		                   (let [[w r] (write-read 10)]
		                     r => w)))
                   
             ))

          (fact "Test offer, roll on segment 5 times, leave data to copy during each roll"
             (let [limit 10
                   path (create-tmp-dir "chronicle" :delete-on-exit true)
                   q (create-queue path limit :segment-limit 20)
                   write-read (fn [x write-n read-n] [(doall
                                         (for [i (range write-n)]
                                           (let [msg (str "msg-" x "-" i)]
                                             (offer q (.getBytes msg) 100)
                                             msg
                                           )))
                                       
                                       (doall
                                         (map #(String. %) (filter (complement nil?) (repeatedly read-n #(poll q 100) ) ))
                                         )])
                                
                                       
                   ]
                   
                   (doall
                     (dotimes [i 10]
                       (let [[w r] (write-read 0 10 5)]
                         r => ["msg-0-0" "msg-0-1" "msg-0-2" "msg-0-3" "msg-0-4"])
                       (let [[w r] (write-read 0 10 5)] 
                         r => ["msg-0-5" "msg-0-6" "msg-0-7" "msg-0-8" "msg-0-9"])))
                   
             ))
          
           (fact "Test offer, roll on segment 5 times, leave data to copy during each roll"
             (let [limit 10
                   path (create-tmp-dir "chronicle" :delete-on-exit true)
                   q (create-queue path limit :segment-limit 20)
                   write-read (fn [x write-n read-n] [(doall
                                         (for [i (range write-n)]
                                           (let [msg (str "msg-" x "-" i)]
                                             (if (offer q (.getBytes msg) 100) msg nil)
                                           )))
                                       
                                       (doall
                                         (map #(String. %) (filter (complement nil?) (repeatedly read-n #(poll q 100) ) ))
                                         )])
                                
                                       
                   ]
                   
                   (let [[w r] (write-read 0 10 5)]
                         r => ["msg-0-0" "msg-0-1" "msg-0-2" "msg-0-3" "msg-0-4"])
                   
                   (let [[w r] (write-read 0 20 0)]
                     ;we expect the write to timeout and only return the messages that it could send.
                     (count (filter (complement nil?) w)) => 6)
                       
                   
             ))
           
       )

