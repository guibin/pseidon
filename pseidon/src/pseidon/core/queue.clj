(ns pseidon.core.queue
  (:use pseidon.core.conf)
  )

(def ^java.util.concurrent.ExecutorService queue-exec (java.util.concurrent.Executors/newCachedThreadPool))
(def ^java.util.concurrent.ExecutorService queue-master (java.util.concurrent.Executors/newCachedThreadPool))


(defn submit [f]
  "Submits a function to a thread pool"
  (fn [msg]
    (let [^java.util.concurrent.Callable callable #(f msg)]
    (.submit queue-exec callable))))

(defn get-worker-queue []
  (java.lang.Class/forName (name (get-conf2 :worker-queue 'java.util.concurrent.PriorityBlockingQueue)) )
  )

(defn ^java.util.concurrent.BlockingQueue channel [] (let [^java.lang.Class cls (get-worker-queue) ] (.newInstance cls)))

(defn consume [^java.util.concurrent.BlockingQueue channel f]
  "Consumes asynchronously from the channel"
  (let [sI (repeatedly #(.take channel))
        ^java.util.concurrent.Callable callable #(doseq [msg sI] (f msg))]
        (.submit queue-master callable)))

(defn publish [^java.util.concurrent.BlockingQueue channel msg]
  (.add channel msg))

(defn publish-seq [^java.util.concurrent.BlockingQueue channel xs]
 (doseq [msg xs] (publish channel msg))
 )
 
(defn qpeek [^java.util.concurrent.BlockingQueue channel]
  (.peek channel)
  )
  