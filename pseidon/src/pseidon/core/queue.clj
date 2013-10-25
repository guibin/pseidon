(ns pseidon.core.queue
  (:require [clojure.tools.logging :refer [info]]
            [pseidon.core.metrics :refer [add-histogram add-gauge update-histogram add-timer measure-time add-meter update-meter]])
  (:use pseidon.core.conf)
  (:import 
          [reactor.queue QueuePersistor IndexedChronicleQueuePersistor]
          [java.util.concurrent ThreadFactory BlockingQueue Callable ThreadPoolExecutor SynchronousQueue TimeUnit ExecutorService ThreadPoolExecutor$CallerRunsPolicy]
          [clojure.lang IFn]
          [java.util.concurrent TimeoutException])
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
(doto (Runtime/getRuntime) (.addShutdownHook (Thread. shutdown-threads)))

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
    (let [^Callable callable (fn[] (try (measure-time exec-timer #(f msg)) 
                                                          (finally (update-meter queue-consume-meter))))
          ^ExecutorService service (get-exec-service (:topic msg))]
     
    (.submit service callable))))

(defprotocol IBlockingChannel 
             (doPut [this e timeout] "Puts an element on the queue potentially blocking")
             (getIterator [this] "Gets an interator from which to consume")
             (close [this]))

(defrecord BlockingChannelImpl [^QueuePersistor persistor ^long limit]
           IBlockingChannel
           (doPut [this msg timeout]
                ;sleep while the queue is full
                  (locking persistor
                    (let [s (System/currentTimeMillis)]
	                    (while (>= (.size persistor) limit)
	                      (Thread/sleep 500)
	                      (if (> (- (System/currentTimeMillis) s) timeout)
                         (throw (TimeoutException. (str "Timined out waiting for queue " timeout))))
	                      )))
                (-> persistor .offer (.apply msg)))
           (getIterator [this ]
                (.iterator persistor))
           (close [this]
                (.close persistor))
           )

(defn close-channel [^BlockingChannelImpl channel]
  (.close channel))

(defn get-worker-queue []
  (let [path  (get-conf2 :pseidon-queue-path (str "/tmp/data/pseidonqueue/" (System/currentTimeMillis)))]
    (info "Creating queue with path " path)
    (IndexedChronicleQueuePersistor. path)))

(defn ^BlockingChannelImpl channel [^String name & {:keys [limit] :or {limit 50}}] 
  (prn "Creating channel " name " :psedon-queue-limit " (get-conf2 :psedon-queue-limit limit))
  (let [
        ^QueuePersistor queue (get-worker-queue)]
        (add-gauge (str "pseidon.core.queue." name ".size") #(.size queue))
        (BlockingChannelImpl. queue (get-conf2 :psedon-queue-limit limit))
        ))

(defn- consume-messages [^BlockingChannelImpl channel ^IFn f]
    (loop [it (.getIterator channel)]
      (while (and (not (.hasNext it)) (not (Thread/interrupted))) (Thread/sleep 500))
      (f (.next it))
      (recur it)))
      

(defn consume [^BlockingChannelImpl channel f]
  "Consumes asynchronously from the channel"
  (let [ 
        ^Runnable runnable #(consume-messages channel f)    
                                 ]
        (.submit queue-master runnable)))

(defn publish [^BlockingChannelImpl channel msg & {:keys [timeout] :or {timeout (Long/MAX_VALUE)}} ]
  (update-meter queue-publish-meter)
  (.doPut channel msg timeout))

(defn publish-seq [^BlockingChannelImpl channel xs]
 (doseq [msg xs] (publish channel msg))
 )
 
  