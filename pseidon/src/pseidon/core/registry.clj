(ns pseidon.core.registry)

(defrecord DataSource [name close list-files reader])

(defn close-ds [ds]
  ((:close ds)))

(defn list-files [ds]
  "calls list-files on a datasource the data source must have a key :list-files"
  ( (:list-files ds) )
)


(def reg-state (agent {}))

(defn assoc2 [m & xs]
  (prn "registered " m)
  (apply assoc m xs)
  )

(defn register-ds [ds]
   (send reg-state assoc2 (keyword (:name ds)) ds)
)

(defn get-ds [name]
  ((keyword name) @reg-state))