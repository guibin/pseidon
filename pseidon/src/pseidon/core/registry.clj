(ns pseidon.core.registry 
  (:use clojure.tools.logging
        pseidon.core.watchdog)
  )

(defrecord DataSource [name start stop list-files reader])
(defrecord Channel [name start stop])
(defrecord DataSink [name start stop writer])
(defrecord Processor [name start stop exec])

(def reg-state (ref {}))

(defn register [{name :name :as item}]
  "Register a service "
   (info "Regiser service " name  (class item))
  (dosync (alter reg-state (fn [p] (assoc p name item) ) ))
  )


(defn reg-get [name]
  "Gets a service registered"
   (get @reg-state name)
  )


  (defn start-all []
    (doseq [[name {start :start}] @reg-state]
        (watch-critical-error start)
        ))
  
  (defn stop-all []
     (doseq [[name {stop :stop}] @reg-state]
          (watch-normal-error stop)
          ))