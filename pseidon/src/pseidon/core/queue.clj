(ns pseidon.core.queue
  (:require [clojure.tools.logging :refer [info error]]
            [pseidon.core.metrics :refer [add-histogram add-gauge update-histogram add-timer measure-time add-meter update-meter]]
            [pseidon.core.chronicle :as chronicle]
            [pseidon.core.watchdog :as watchdog])
  
  (:use pseidon.core.conf)
  (:import 
          [java.util.concurrent ThreadFactory BlockingQueue Callable ThreadPoolExecutor SynchronousQueue TimeUnit ExecutorService ThreadPoolExecutor$CallerRunsPolicy]
          [clojure.lang IFn]
          [java.util Iterator]
          [java.util.concurrent TimeoutException ArrayBlockingQueue]
          [pseidon.util Bytes Encoder Decoder DefaultEncoder DefaultDecoders])
  )

(def topic-services (ref {}))

;the master threads are daemon threads to allow the jvm to shutdown at any time
(def ^ExecutorService queue-master (java.util.concurrent.Executors/newCachedThreadPool
                                     (reify ThreadFactory
                                       (^Thread newThread [_ ^Runnable r]
                                         (doto (Thread. r) (.setDaemon true))))))

(def exec-timer (add-timer "pseidon.core.queue.exec-timer"))
(def queue-publish-meter (add-meter "pseidon.core.queue.publish-meter"))
(def queue-consume-meter (add-meter "pseidon.core.queue.consume-meter"))


(defn shutdown-threads []
  (.shutdown queue-master)
  (doseq [[_ service] @topic-services]
    (.shutdown service)
    (info "shutdown " service) 
    (if-not (.awaitTermination service 1 TimeUnit/SECONDS)
      (.shutdownNow service))))

;on jvm shutdown we shutdown all threads
;(doto (Runtime/getRuntime) (.addShutdownHook (Thread. shutdown-threads)))

(defn- create-exec-service [topic]
  (let [threads (get-conf2 (keyword (str "worker-" topic "-threads")) (get-conf2 :worker-threads (-> (Runtime/getRuntime) .availableProcessors)))]
    (info "Creating thread pool for " topic " limit " threads)
      (doto (ThreadPoolExecutor. 0 threads 60 TimeUnit/SECONDS (SynchronousQueue.))
        (.setRejectedExecutionHandler  (ThreadPoolExecutor$CallerRunsPolicy.)))))
       
    
(defn ^ExecutorService get-exec-service [^String topic]
    (dosync
      (if-let [service (get @topic-services topic)] service
        (get (alter topic-services assoc topic (create-exec-service topic)) topic ))))
  
(defn submit [f]
  "Submits a function to a thread pool"
  (fn [msg]
    
    (if-not (:topic msg)
      (throw (RuntimeException. (str "Topic cannot be nil for message " msg " check that you've registered the MSG-DECODER with the consumer"))))
    
    (let [^Callable callable (fn[] (try (measure-time exec-timer #(f msg)) 
                                                          (finally (update-meter queue-consume-meter))))
          ^ExecutorService service (get-exec-service (:topic msg))]
     
    (.submit service callable))))

(defprotocol IBlockingChannel 
             (doPut [this e timeout])
             (doPut! [this e]) 
             (getIterator [this])
             (getSize [this])
             (close [this])
            
             )

(defrecord BlockingChannelImpl [chronicle]
           IBlockingChannel
           (doPut [this msg timeout]
             (chronicle/offer chronicle msg timeout))
           (doPut! [this msg]
             (chronicle/offer! chronicle msg)
             true)
           (getIterator [this]
                (chronicle/create-iterator chronicle))
           (getSize [this]
             (chronicle/get-size chronicle))
           (close [this]
                (info "calling chronicle close >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
                (chronicle/close chronicle))
           )

(defrecord ArrayBlockingQueueChannel [^ArrayBlockingQueue queue]
  IBlockingChannel
  (doPut [this msg timeout]
    (.offer queue msg timeout TimeUnit/MILLISECONDS))
  (doPut! [this msg]
    (.put queue msg)
    true)
  (getIterator [this]
    (reify Iterator
      (hasNext [this] true)
      (remove [this])
      (next [this]
        (take queue))))
  (getSize [this]
     (.size queue))
  (close [this]
    ))

(defn create-blocking-array-queue [limit]
  (ArrayBlockingQueueChannel. (ArrayBlockingQueue. limit)))

(defn close-channel [^BlockingChannelImpl channel]
  (info "call close on channel")
  (close channel))

(defn get-worker-queue [& {:keys [limit buffer queue-type decoder encoder] :or {limit (get-conf2 :pseidon-queue-limit 100) buffer (get-conf2 :pseidon-queue-buff 100)
                                                                                     queue-type (get-conf2 :psiedon-queue-type "chronicle")
                                                                                     decoder DefaultDecoders/BYTES_DECODER
                                                                                     encoder DefaultEncoder/DEFAULT_ENCODER} }]
  (let [path  (get-conf2 :pseidon-queue-path (str "/tmp/data/pseidonqueue/" (System/currentTimeMillis)))
       segment-limit (get-conf2 :pseidon-queue-segment-limit 1000000)
       
       queue (cond (= queue-type "blocking-array") (create-blocking-array-queue  limit)
                   :else (BlockingChannelImpl. (chronicle/create-queue path limit decoder encoder :buffer buffer :segment-limit segment-limit)))
       ]
    (info "Creating queue with path " path " queue class " (type queue))
    queue
    ))

(defn channel [^String name & {:keys [limit buffer decoder encoder] :or {limit (get-conf2 :pseidon-queue-limit 100) buffer (get-conf2 :pseidon-queue-buff 100)
                                                                         decoder DefaultDecoders/BYTES_DECODER
                                                                         encoder DefaultEncoder/DEFAULT_ENCODER}} ] 
  (info "Creating channel " name " :psedon-queue-limit " limit " buffer " buffer)
  (let [
        queue (get-worker-queue :limit limit :buffer buffer :decoder decoder :encoder encoder)]
        (add-gauge (str "pseidon.core.queue." name ".size") #(getSize queue))
        queue))

(defn- consume-messages [^BlockingChannelImpl channel ^IFn f]
    (loop [^Iterator it (.getIterator channel)]
        (while (and (not (.hasNext it)) (not (Thread/interrupted))) (Thread/sleep 100))
        (if it 
          (f (.next it)))
        (recur it)))
      

(defn consume [channel f]
  "Consumes asynchronously from the channel"
  (let [ 
        ^Runnable runnable #(try 
                              (consume-messages channel f)
                              (catch Exception e (watchdog/handle-critical-error e "Error while consuming")))
                                 ]
        (.submit queue-master runnable)))


(defn publish [ channel msg & {:keys [timeout] :or {timeout -1}} ]
   (update-meter queue-publish-meter)
   (if (pos? timeout)
     (if-not (doPut channel msg timeout)
       (throw (TimeoutException. (str "publish-bytes timeout after " timeout " ms"))))
     (doPut! channel msg)))
  

(defn publish-seq [ channel xs & {:keys [timeout] :or {timeout -1}}]
 (doseq [msg xs] (publish channel msg :timeout timeout ))
 )
 
  