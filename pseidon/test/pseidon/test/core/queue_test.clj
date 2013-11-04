(ns pseidon.test.core.queue-test
  
  (:require [pseidon.core.queue :refer :all])
  (:use midje.sweet)
  (:import [java.util.concurrent Executors TimeUnit TimeoutException]
           [pseidon.util Bytes DefaultDecoders])
  
  )

(facts "Test pub sub"
       
       (fact "Test pub sub timeout"

             (let [
                   messages (range 100)
                   ch (channel "test" :limit 10 :buffer -1)
                   received-msgs (ref [])
                   ]
                 
               (try 
                 (doseq [msg messages]
                   (prn "publish " msg)
                   (publish ch msg :timeout 10))
                (catch TimeoutException e (prn "planned timeout")))
               (Thread/sleep 1000)
               
               (consume ch (fn [msg]
                             (dosync
                               (commute received-msgs conj msg))
                             )
                             :decoder DefaultDecoders/LONG_DECODER)
               
               (Thread/sleep 1000)
               
               (prn "messages " @received-msgs  " taken " (vec (sort (conj (take 11 messages) 11))))
               ;we conj 11 because this is the message that caused the timeout
               (= (sort @received-msgs) (vec (sort (conj (take 11 messages) 11))))  => true
               (close-channel ch)
               
               ))
       
       (fact "Test pub sub"

             (let [
                   messages (range 10)
                   ch (channel "test")
                   received-msgs (ref [])
                   ]
               (consume ch (fn [msg]
                             (dosync
                               (commute received-msgs conj msg)))
                        :decoder DefaultDecoders/LONG_DECODER)
                               
               (doseq [msg messages]
                 (prn "publish " msg)
                 (publish ch msg))
               
               (Thread/sleep 1000)
               
               (prn "messages " @received-msgs)
               (= (sort @received-msgs) messages) => true
               (close-channel ch)
               
               ))
       
       (fact "Test pub sub multi threaded"

             (let [
                   service (Executors/newCachedThreadPool)
                   ch (channel "test1223" :limit 10000)
                   received-msgs (ref [])
                   ]
               (consume ch (fn [msg]
                             (dosync
                               (commute received-msgs conj msg)))
                        :decoder DefaultDecoders/STR_DECODER)
               
               (doseq [msg (range 100)]
                 (.submit service (reify Runnable
                                    (run [this]
                                      (dotimes [i 100] 
                                        (publish ch (str msg "-" i)))))))
               
               (.shutdown service)
               (.awaitTermination service 1 TimeUnit/SECONDS)
               (Thread/sleep 1000)
               
               (count @received-msgs) => 10000
               (close-channel ch)
               
               )))
      
       
               

