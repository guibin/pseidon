(ns pseidon.core)

(def exec (java.util.concurrent.Executors/newCachedThreadPool))

(def master (java.util.concurrent.Executors/newCachedThreadPool))

(defn submit [f]
  "Submits a function to a thread pool"
  (fn [msg](.submit exec #(f msg)))
  )

(defn channel [] (java.util.concurrent.PriorityBlockingQueue.))

(defn consume [channel f]
  "Consumes asynchronously from the channel"
  (let [sI (repeatedly #(.take channel))]
  (.submit master #(doseq [msg sI]
    (f msg)
   ))))



(defn publish [channel msg]
  (.add channel msg))

(def ch (channel))

(consume ch (submit #(prn "hey->" %1)))
