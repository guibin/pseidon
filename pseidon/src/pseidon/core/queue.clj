(ns pseidon.core.queue
  (:use pseidon.core.conf)
  )

(def exec (java.util.concurrent.Executors/newCachedThreadPool))
(def master (java.util.concurrent.Executors/newCachedThreadPool))


(defn submit [f]
  "Submits a function to a thread pool"
  (fn [msg](.submit exec #(f msg)))
  )

(defn get-worker-queue []
  (java.lang.Class/forName (name (get-conf2 :worker-queue 'java.util.concurrent.PriorityBlockingQueue)) )
  )

(defn channel [] (let [^java.lang.Class cls (get-worker-queue) ] (.newInstance cls)))

(defn consume [channel f]
  "Consumes asynchronously from the channel"
  (let [sI (repeatedly #(.take channel))]
  (.submit master #(doseq [msg sI]
    (f msg)
   ))))

(defn publish [channel msg]
  (.add channel msg))

(defn publish-seq [channel xs]
 (doseq [msg xs] (publish channel msg))
 )
 
(defn qpeek [channel]
  (.peek channel)
  )
  