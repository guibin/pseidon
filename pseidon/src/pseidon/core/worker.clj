(ns pseidon.core.worker)
(use '[clojure.tools.logging])
(use '[pseidon.core.queue :as q])
(use '[pseidon.core.registry :as r])

;This module contains methods that help with the worker delegation from the queue and consumption.

;runs the processor's exec function passing it a msg instance
(defn process-msg [{:keys [exec] }, msg]
   (exec msg)
  )

;delagate to workers
(defn delegate-msg [msg]
  (try 
    (doseq [topic (.-topics msg)]
      (process-msg (r/reg-get topic) msg)
     )    (catch Exception e (error "ERROR @TO FIX implement an error handling sink " e)))
  )

(defn start-consume [channel]
  "Start consuming from the channel this method runs async"
   (consume channel (submit delegate-msg))
   true
  )
