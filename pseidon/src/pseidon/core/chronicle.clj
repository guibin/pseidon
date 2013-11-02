(ns pseidon.core.chronicle
  (:require [clojure.core.async :refer [go chan >!! <!! close! <! >!]]
            [pseidon.core.utils :refer [not-interrupted]])
  (:import [net.openhft.chronicle IndexedChronicle ChronicleConfig]
           [java.util.concurrent.atomic AtomicInteger]
           [java.io File])
  )

(defn path-to-str [path]
  (if (instance? File path)
    (.getAbsolutePath path)
    path))

(defprotocol ChronicleQueue 
  (offer [this msg])
  (close [this])
  (get [this]))

(defrecord ChronicleQueueImpl [write-ch read-ch]
  ChronicleQueue
  (offer [this msg]
    (>!! write-ch msg))
  
  (get [this]
    ))

(defn ^IndexedChronicle create-chronicle [path]
    (let [ chronicle (IndexedChronicle. (path-to-str path) (ChronicleConfig/SMALL))
           appender (.createAppender chronicle)
           tailer (.createTailer chronicle)]
      {:chronicle chronicle :appender appender :tailer tailer}))
        
(defn block-on-size [^AtomicInteger size limit]
  (while (>=  (.get size) limit)
    (Thread/sleep 200)))

(defn copy-ch [read-ch new-chronicle]
  (let [[v ch] (alts!! [c (timeout 200)])]
    (if (= ch read-ch)
      (do 
        (write-to-chronicle new-appender v)
        (recur read-ch new-chronicle)))))

(defn check-roll-chronicle! [chronicle-ref path read-ch ^AtomicInteger queue-size ^long segment-limit]
  (if (> (.get queue-size) segment-limit)
    (do 
      (let [{:keys [chronicle appender tailer]} @chronicle-ref
            new-chronicle (create-chronicle path)]
        ;we must read all left over messages from the old channel into the new one
        (copy-ch read-ch new-chronicle)
        (prn "closing !!!!")
        (.set queue-size 0)
        (.close appender)
        (.close tailer)
        (.close chronicle))
      
      (dosync (ref-set chronicle-ref new-chronicle)))))
    
(defn write-to-chronicle [{:keys [^IndexedChronicle chronicle appender tailer]} ^bytes msg]
  (doto 
    appender
    .startExcerpt
    (.writeInt (count msg))
    (.write msg)
    .finish))
 
 (defn ^bytes read-from-chronicle [tailer]
   (let [size (.readInt tailer)
         ^bytes bts (byte-array size)]
     (.read tailer bts)
     bts))

(defn create-queue [path limit & {:keys [segment-limit] :or {:segment-limit 1000000}}]
  (let [write-ch (chan)
        read-ch (chan)
        chronicle-ref (ref (create-chronicle path))
        queue-size (AtomicInteger.)
        ]
    (go 
      (while (not-interrupted)
        
        (let [^bytes msg (<! write-ch)]
          (prn "writing " (.get queue-size))
          (block-on-size queue-size limit)
          (check-roll-chronicle! chronicle-ref path queue-size segment-limit)
          (.incrementAndGet queue-size)
          (write-to-chronicle @chronicle-ref msg))))
    (go (while (not-interrupted)
        (let [{tailer :tailer} @chronicle-ref]
          (loop [t tailer]
            (if (.nextIndex t)
              (read-from-chronicle tailer))
            (recur t)))))
          
    ; ChronicleQueueImpl [write-ch read-ch]
    (ChronicleQueueImpl. write-ch read-ch)))

