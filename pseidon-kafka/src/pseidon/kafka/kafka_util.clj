(ns pseidon.kafka.kafka-util
  (:require [clojure.tools.logging :refer [info error]])
  (:import
          [java.nio ByteBuffer]
          [java.util Properties]
          [kafka.message MessageAndMetadata MessageAndOffset]
          [java.util.concurrent LinkedBlockingQueue]
          [kafka.cluster Broker]
	  )
  )


(defrecord KafkaMessage [topic offset partition key value])

(defprotocol StringUtil (as-string [this]))

(extend-protocol StringUtil
               nil
                    (as-string [_] "")
               Object
		     (as-string [this] (str this))
               java.util.Collection
                     (as-string [this] (clojure.string/join "," this))
               java.util.Map
                     (as-string [this] (as-string (vals this))))

(defn as-properties
  [m]
  (let [props (Properties. )]
    
    (doseq [[n v] m] (if (and v v) (.setProperty props (str n)  (as-string v))))
    props))

(defprotocol ToClojure
  (to-clojure [x] "Converts type to Clojure structure"))

(extend-protocol ToClojure
  
  KafkaMessage
  (to-clojure [x] x)
  
  MessageAndMetadata
  (to-clojure [x] (KafkaMessage. (.topic x) (.offset x) (.partition x) (.key x) (.message x)))

  MessageAndOffset
  (to-clojure [x]
    (letfn [(byte-buffer-bytes [^ByteBuffer bb] (let [b (byte-array (.remaining bb))]
                                      (.get bb b)
                                      b))]
      (let [offset (.offset x)
            msg (.message x)]
        (KafkaMessage. nil offset nil (.key msg) (byte-buffer-bytes (.payload msg))))))

  Broker
  (to-clojure [x]
    {:connect (.getConnectionString x)
     :host (.host x)
     :port (.port x)
     :broker-id (.id x)})


  )

(defmacro with-resource
  [binding close-fn & body]
  `(let ~binding
     (try
       (do ~@body)
       (finally
        (~close-fn ~(binding 0))))))


(defn pipe
  "Returns a vector containing a sequence that will read from the
   queue, and a function that inserts items into the queue.

   Source: http://clj-me.cgrand.net/2010/04/02/pipe-dreams-are-not-necessarily-made-of-promises/"
  ([] (pipe 100))
  ([size]
   (let [q (java.util.concurrent.LinkedBlockingQueue. ^int size)
         EOQ (Object.)
         NIL (Object.)
         s (fn queue-seq [] (lazy-seq
                              (let [x (.take q)]
                                (when-not (= EOQ x)
                                  (cons (when-not (= NIL x) x)
                                        (queue-seq))))))]
     [(s) (fn queue-put
            ([] (.put q EOQ))
            ([x] (.put q (or x NIL))))])))
