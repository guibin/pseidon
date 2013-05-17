(ns pseidon.core.registry)

(defrecord DataSource [name close list-files reader])
(defrecord Channel [name start stop])
(defrecord DataSink [name close writer])
(defrecord Processor [topic start stop exec])

(defn close-ds [ds]
  ((:close ds)))

(defn list-files [ds]
  "calls list-files on a datasource the data source must have a key :list-files"
  ( (:list-files ds) )
)


(def reg-state (ref {}))
(def channel-state (ref {}))
(def sink-state (ref {}))
(def processor-state (ref {}))


(defn register-ds [ds]
   (prn "!!!!!!Register Data source " ds)
   (dosync (alter reg-state (fn [p] (conj p {(keyword (:name ds)) ds}) ) ))
   reg-state
   )

(defn get-ds [name]
  ((keyword name) @reg-state))

(defn get-sink [name]
    ((keyword name) @sink-state))

(defn get-processor [topic]
    ((keyword topic) @processor-state))

(defn register-channel [ch]
   (dosync (alter channel-state (fn [p] (conj p {(keyword (:name ch)) ch} ) ) )
   ))

(defn register-sink [sink]
   (dosync (alter sink-state (fn [p] (conj p {(keyword (:name sink)) sink})) ) )
   )

(defn register-processor [processor]
   (dosync (alter processor-state (fn [p] (conj p {(keyword (:topic processor)) processor})) ) )
   )


(defn stop-ds [] 
  (doseq [[k,ds] @reg-state]
     (close-ds ds)))

(defn stop-sinks[]
  (doseq [[k,sink] @sink-state]
     ((:close sink))))

(defn stop-channels [] 
  (doseq [[k,ch] @channel-state]
     ((:stop ch))))

(defn start-channels []
  (doseq [[k,ch] @channel-state]
     (prn ch)
     ((:start ch))
    )
  )

(defn stop-processors [] 
  (doseq [[k,ch] @processor-state]
     ((:stop ch))))

(defn start-processors []
  (doseq [[k,ch] @processor-state]
     ((:start ch))
    )
  )
