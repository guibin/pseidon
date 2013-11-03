(ns pseidon.core.chronicle
  (:require [clojure.core.async :refer [go chan alts!! >!! <!! close! <! >! timeout]]
            [clojure.java.io :refer [file]]
            [clojure.tools.logging :refer [info error]]
            [pseidon.core.utils :refer [not-interrupted]])
  (:import [net.openhft.chronicle IndexedChronicle ChronicleConfig]
           [java.util.concurrent.atomic AtomicInteger]
           [org.apache.commons.io FileUtils]
           [java.io File])
  )

(declare write-to-chronicle)

(defn path-to-str [path]
  (if (instance? File path)
    (.getAbsolutePath path)
    path))

(defprotocol ChronicleQueue 
  (offer! [this msg])
  (offer [this msg timeout-msg])
  (close [this])
  (poll! [this])
  (poll [this timeout-ms]))

(defrecord ChronicleQueueImpl [write-ch read-ch chronicle-ref]
  ChronicleQueue
  (offer! [this msg]
    (>!! write-ch msg))
  (offer [this msg timeout-ms]
    "Blocks till the message can be placed on the queue and returns true otherwise returns false"
    (not 
       (nil? 
         (first (alts!! [(go (>! write-ch msg) msg) (timeout timeout-ms)])))))

  (poll! [this]
    "Poll for a value and blocks untill a value is available"
    (<!! read-ch))
  (poll [this timeout-ms]
    "Retursn nil if timeout otherwise return a value"
    (let [[v _] (alts!! [read-ch (timeout timeout-ms)])]
          v))
          
  (close [this]
    (let [{:keys [chronicle appender tailer]} @chronicle-ref]
      (.close appender)
      (.close tailer)
      (.close chronicle)))
    )

(defn ^File get-latest-chronicle-dir [parent-dir]
  "Takes as argument the parent directory of the chronicle indexes and looks for the 
   latest directory"
  (last
    (sort-by #(.lastModified %) (file-seq (file parent-dir)))))

(defn load-chronicle-path [path]
  (get-latest-chronicle-dir path))
    
(defn ^String new-chronicle-path [path]
  (str (path-to-str path) "/" (System/currentTimeMillis) "/queue"))
           
(defn ^IndexedChronicle create-chronicle [path]
    (let [ chronicle (IndexedChronicle. (path-to-str path) (ChronicleConfig/SMALL))
           appender (.createAppender chronicle)
           tailer (.createTailer chronicle)]
      (info "Creating chronicle dir " path  )
      {:chronicle chronicle :appender appender :tailer tailer :queue-path path}))
        
(defn block-on-size [^AtomicInteger size limit]
  (while (> (.get size) limit)
    (do 
      (Thread/sleep 200))))

(defn copy-ch [read-ch new-chronicle i]
  "Copies from the read channel to the new chronicle
   retrns the amount of records copied"
  (let [[v ch] (alts!! [read-ch (timeout 200)])]
    (if (= ch read-ch)
      (do 
        ;(info "copy " i)
        (write-to-chronicle new-chronicle v)
        (recur read-ch new-chronicle (inc i) ))
        i)))

(defn check-roll-chronicle! [chronicle-ref path read-ch ^AtomicInteger queue-size ^AtomicInteger segment-size segment-limit]
  "Check if we should roll the chronicle file
   If so this function will create a new chronicle, copy any data left in the previous chronicle and deletes the previous"
  (if (> (.get segment-size) segment-limit)
    (do 
      (let [{:keys [chronicle appender tailer queue-path]} @chronicle-ref
            new-chronicle (-> path (new-chronicle-path) (create-chronicle))]
        ;we must read all left over messages from the old channel into the new one
        
        (let [len (copy-ch read-ch new-chronicle 0)]
          (info "copied left over from previous chronicle " len)
          (.set segment-size len)
          (.set queue-size len))
        
        (future
          ;do cleanup in a separate thread
	        (.close appender)
	        (.close tailer)
	        (.close chronicle)
	        (-> queue-path (file ) (.getParentFile) (FileUtils/deleteDirectory)))
	        
        (info "delete dir : " (-> queue-path (file ) (.getParentFile)))
        (dosync (ref-set chronicle-ref new-chronicle))))))

    
(defn write-to-chronicle [{:keys [^IndexedChronicle chronicle appender tailer]} ^bytes msg]
  "Write the bytes message to the chronicle appender"
  (doto 
    appender
    .startExcerpt
    (.writeInt (count msg))
    (.write msg)
    .finish))
 
 (defn ^bytes read-from-chronicle [tailer]
   "Read a bytes message from tailer"
   (let [size (.readInt tailer)
         ^bytes bts (byte-array size)]
     (.read tailer bts)
     bts))

(defn create-queue [path limit & {:keys [segment-limit] :or {segment-limit (* 2 limit)}}]
  "Returns a ChronicleQueue with background writters and readers enabled"
  (let [segment-limit2 (if (> segment-limit limit) segment-limit (do (info "segment limit cannot be smaller than the limit setting to 2 * limit") 
                                                                   (* 2 limit)))
        write-ch (chan)
        read-ch (chan)
        chronicle-ref (ref (if-let [p (load-chronicle-path path)] (create-chronicle p) (create-chronicle (new-chronicle-path path)  )))
        queue-size (AtomicInteger.)
        segment-size (AtomicInteger.)
        ]
    (go 
      (while (not-interrupted)
        
        (let [^bytes msg (<! write-ch)]
          ;(info "on write-ch " (String. msg))
          (.incrementAndGet queue-size) ;the current queue size
          (.incrementAndGet segment-size) ; count before rolling on a segment
          
          (block-on-size queue-size limit)
          ;(info "after block-on-size on write-ch " (String. msg))
          
          (check-roll-chronicle! chronicle-ref path read-ch queue-size segment-size segment-limit2)
          
          (write-to-chronicle @chronicle-ref msg))))
    
    ;we loop forever, get a reference value from the chronicle-ref
    ;and get the tailer, loop through the tailer until it returns no values
    ;then go back and derefence the chronicle-ref again to get a new tailer, or the same.
    ;If the chronicle was rolled the current tailer will stop returning values and 
    ;the chronicle-ref will have a new chronicle instance with tailer assigned.
    (go 
      (while (not-interrupted)
        (try
          (let [{tailer :tailer} @chronicle-ref]
            (loop [t tailer]
              (if (.nextIndex t) 
                (do 
                  (>! read-ch (read-from-chronicle tailer))
                  (.decrementAndGet queue-size)
                  (recur t)))))
          (catch Exception e (error e e)))))
          
    ; ChronicleQueueImpl [write-ch read-ch]
    (ChronicleQueueImpl. write-ch read-ch chronicle-ref)))

