(ns pseidon.core.queue
  (:require [clojure.tools.logging :refer [info]]
            [pseidon.core.metrics :refer [add-histogram add-gauge update-histogram add-timer measure-time add-meter update-meter]])
  (:use pseidon.core.conf)
  (:import [java.util.concurrent BlockingQueue Callable ThreadPoolExecutor SynchronousQueue TimeUnit ExecutorService ThreadPoolExecutor$CallerRunsPolicy])
  )

(def topic-services (ref {}))

(def ^ExecutorService queue-master (java.util.concurrent.Executors/newCachedThreadPool))

(def exec-timer (add-timer "pseidon.core.queue.exec-timer"))
(def queue-publish-meter (add-meter "pseidon.core.queue.publish-meter"))
(def queue-consume-meter (add-meter "pseidon.core.queue.consume-meter"))


(defn shutdown-threads []
  (.shutdown queue-master)
  (doseq [[_ service] @topic-services]
    (.shutdown service)
    (if-not (.awaitTermination service 10 TimeUnit/SECONDS)
      (.shutdownNow service))))

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

(defn get-worker-queue []
  (java.lang.Class/forName (name (get-conf2 :worker-queue 'java.util.concurrent.ArrayBlockingQueue)) )
  )

(defn ^BlockingQueue channel [^String name] 
  (let [limit (get-conf2 :worker-queue-limit 1000)
        ^BlockingQueue queue (let [^Class cls (get-worker-queue) ] 
                               (try 
                                 (-> cls (.getConstructor (into-array Class [Integer/TYPE])) (.newInstance (into-array Object [(int limit)])))
                                 (catch NoSuchMethodException e (.newInstance cls) )  ))]
        (add-gauge (str "pseidon.core.queue." name ".size") #(.size queue))
        queue
        ))

(defn consume [^BlockingQueue channel f]
  "Consumes asynchronously from the channel"
  (let [sI (repeatedly #(.take channel))
        ^Callable callable #(doseq [msg sI] (f msg))]
        (.submit queue-master callable)))

(defn publish [^BlockingQueue channel msg]
  (update-meter queue-publish-meter)
  (.offer channel msg 180 TimeUnit/SECONDS))

(defn publish-seq [^java.util.concurrent.BlockingQueue channel xs]
 (doseq [msg xs] (publish channel msg))
 )
 
(defn qpeek [^java.util.concurrent.BlockingQueue channel]
  (.peek channel)
  )
  