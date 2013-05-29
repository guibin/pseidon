(ns pseidon.core.worker)
(use '[clojure.tools.logging])
(use '[pseidon.core.queue :as q])
(use '[pseidon.core.registry :as r])
(use '[pseidon.core.watchdog :as w])

;This module contains methods that help with the worker delegation from the queue and consumption.

;runs the processor's exec function passing it a msg instance
(defn process-msg [{:keys [exec] }, msg]
   (exec msg)
  )

;delagate to workers
(defn delegate-msg [msg]
    (let [topic (:topic msg)
          exec (:exec (r/reg-get topic) ) ]
     (exec msg)   
   ) )

(defn start-consume [channel]
  "Start consuming from the channel this method runs async"
   (consume channel (submit (w/watch-critical-error delegate-msg) ))
   true
  )
