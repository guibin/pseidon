(ns pseidon.core.registry 
  (:use clojure.tools.logging)
  )

(defrecord DataSource [name start stop list-files reader])
(defrecord Channel [name start stop])
(defrecord DataSink [name start stop writer])
(defrecord Processor [name start stop exec])

(def reg-state (ref {}))

(defn register [{name :name :as item}]
  "Register a service "
   (info "Regiser service " name  (class item))
  (dosync (alter reg-state (fn [p] (assoc p (keyword name) item) ) ))
  )


(defn reg-get [name]
  "Gets a service registered"
   ((keyword name) @reg-state)
  )


  (defn start-all []
    (doseq [[name {start :start}] @reg-state]
        
        (start)
        ))
  
  (defn stop-all []
     (doseq [[name {stop :stop}] @reg-state]
          (stop)
          ))