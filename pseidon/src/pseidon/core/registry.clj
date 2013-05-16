(ns pseidon.core.registry)

(defrecord DataSource [name close list-files reader])
(defrecord Channel [start stop])

(defn close-ds [ds]
  ((:close ds)))

(defn list-files [ds]
  "calls list-files on a datasource the data source must have a key :list-files"
  ( (:list-files ds) )
)


(def reg-state (agent {}))
(def channel-state (agent {}))

(defn assoc2 [m & xs]
  (apply assoc m xs)
  )

(defn register-ds [ds]
   (send reg-state assoc2 (keyword (:name ds)) ds)
)

(defn get-ds [name]
  (Thread/sleep 500)
  ((keyword name) @reg-state))

(defn register-channel [ch]
   (send channel-state assoc2 (keyword (:name ch)) ch)
   )

(defn stop-ds [] 
  (doseq [[k,ds] @reg-state]
     (close-ds ds)
    )
  )

(defn stop-channels [] 
  (doseq [[k,ch] @channel-state]
     ((:stop ch))
    )
  )

(defn start-channels []
  (doseq [[k,ch] @channel-state]
     ((:start ch))
    )
  )