(ns pseidon.core.message)

;bytes-f returns a byte array Do not create this record directly 
;rather use the methods provided in the ns, you'll be shielded from future changes.
(defrecord Message [bytes-f ^String topic ^boolean accept ^long ts ^int priority] 
  
  java.lang.Comparable
     (compareTo [this m] 
       (compare (:priority this) (:priority m)))
  )

(defn change-bytes [m bytes-f]
  (->Message  bytes-f (:topic m) (:accept m) (:ts m) (:priority m))
  )

(defn create-message [bytes-f topic accept ts priority]
  (->Message bytes-f topic accept ts priority)
  )
