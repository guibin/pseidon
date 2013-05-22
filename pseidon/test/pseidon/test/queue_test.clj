(ns pseidon.test.queue_test)
(use '[midje.sweet])
(use '[pseidon.core.queue])


(def queue (channel))

(defn publishAndReturn [data]
  (publish queue data)
  (qpeek queue)
  )



(facts "Test the worker queue"
       
       (fact "publish to queue"
             (publishAndReturn "hi") => "hi"
             )
       )

