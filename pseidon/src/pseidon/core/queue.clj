(ns pseidon.core.queue
  (:require [clojure.tools.logging :refer [info]]
            [pseidon.core.metrics :refer [add-histogram add-gauge update-histogram add-timer measure-time add-meter update-meter]])
  (:use pseidon.core.conf)
  )

(def ^java.util.concurrent.ExecutorService queue-exec (java.util.concurrent.Executors/newCachedThreadPool))
(def ^java.util.concurrent.ExecutorService queue-master (java.util.concurrent.Executors/newCachedThreadPool))

(def exec-timer (add-timer "pseidon.core.queue.exec-timer"))
(def queue-publish-meter (add-meter "pseidon.core.queue.publish-meter"))
(def queue-consume-meter (add-meter "pseidon.core.queue.consume-meter"))

  
(defn submit [f]
  "Submits a function to a thread pool"
  (fn [msg]
    (let [^java.util.concurrent.Callable callable (fn[] (try (measure-time exec-timer #(f msg)) 
                                                          (finally (update-meter queue-consume-meter))))]
    (.submit queue-exec callable))))

(defn get-worker-queue []
  (java.lang.Class/forName (name (get-conf2 :worker-queue 'java.util.concurrent.PriorityBlockingQueue)) )
  )

(defn ^java.util.concurrent.BlockingQueue channel [^String name] 
  (let [^java.util.concurrent.BlockingQueue queue (let [^java.lang.Class cls (get-worker-queue) ] (.newInstance cls))]
        (add-gauge (str "pseidon.core.queue." name ".size") #(.size queue))
        queue
        ))

(defn consume [^java.util.concurrent.BlockingQueue channel f]
  "Consumes asynchronously from the channel"
  (let [sI (repeatedly #(.take channel))
        ^java.util.concurrent.Callable callable #(doseq [msg sI] (f msg))]
        (.submit queue-master callable)))

(defn publish [^java.util.concurrent.BlockingQueue channel msg]
  (info "publish " (:topic msg) " to " channel)
  (update-meter queue-publish-meter)
  (.add channel msg))

(defn publish-seq [^java.util.concurrent.BlockingQueue channel xs]
 (doseq [msg xs] (publish channel msg))
 )
 
(defn qpeek [^java.util.concurrent.BlockingQueue channel]
  (.peek channel)
  )
  