(ns pseidon.core.message)

;bytes-f returns a byte array Do not create this record directly 
;rather use the methods provided in the ns, you'll be shielded from future changes.
(defrecord Message [^clojure.lang.IFn bytes-f ^String topic  accept ^long ts ^long priority] 
  
  java.lang.Comparable
     (compareTo [this m] 
       (compare (:priority this) (:priority m)))
  )

(defn change-bytes [^Message m ^clojure.lang.IFn bytes-f]
  (->Message  bytes-f (:topic m) (:accept m) (:ts m) (:priority m))
  )

(defn create-message [^clojure.lang.IFn bytes-f topic accept ts priority]
  (->Message bytes-f topic accept ts priority)
  )

(defn get-bytes [{:keys [bytes-f] }]
  "
   Bytes can be a function or the direct byte array,
   This makes reading bytes at the correct moment more flexible
   "
  (if (instance? clojure.lang.IFn) (bytes-f) bytes-f)
  )