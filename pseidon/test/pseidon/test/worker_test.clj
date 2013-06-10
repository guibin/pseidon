(ns pseidon.test.worker_test)
(use '[midje.sweet])
(use '[pseidon.core.queue])
(use '[pseidon.core.worker])
(use '[pseidon.core.registry])
(comment 
  
(import '[org.streams.streamslog.log.file MessageMetaData])

(def worker-done (ref false))

(def msg  (MessageMetaData. (.getBytes "hi") (into-array ["test"]) true (System/currentTimeMillis) 1))

(def worker (->Processor "test" (fn [] ) (fn []) (fn [msg] (dosync  (ref-set worker-done true )))))

;we register the worker
(register worker)

(facts "Test worker delegation"
       (fact "Test delegate"
             (prn "Test: " (delegate-msg msg))
             (do (delegate-msg msg) @worker-done) => true
             ))

)