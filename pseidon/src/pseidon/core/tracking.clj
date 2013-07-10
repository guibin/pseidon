(ns pseidon.core.tracking
 (:require [cupboard.core :as cb]
           [pseidon.core.conf :refer [get-conf2] ]
           )
 (:use [cupboard.utils])
 (:import 
           [com.sleepycat.je UniqueConstraintException])
  )

(def status-run "run")
(def status-done "done")

(def started? (java.util.concurrent.atomic.AtomicBoolean.))

(defn create-table[]
  (cb/defpersist messagetracking 
    ( (:ts :index :any)
      (:ds-id :index :unique)
      (:status :index :any)      
    ))
  )


(defn start []
  (cb/open-cupboard! (get-conf2 "tracking-db-dir" "/tmp/pseidon-tracking"))
  (create-table)
  
  )

(defn ensure-started []
  "
   Ensures that the db is started
  "
  (if (not (.get started?) ) 
    (if (.compareAndSet started? false true)
      (start) 
      )
    )
  )

(defn mark-run! [^String ds ^String id]
  " The ds and id values cannot hold any byte 1 characters, the key formed is ds byte1 id and must be unique, 
    if the object already exists in the database a unique constraint exception will be thrown.

    This method saves the message tracking metadata with status==run
  "
  (ensure-started)
  (let [ds-id (clojure.string/join \u0001 [ds id] )]
	    (try 
	    (cb/make-instance messagetracking [(System/currentTimeMillis) ds-id status-run])
	    (catch UniqueConstraintException e [] )
	    )
    )
  )

(defn mark-done! [^String ds ^String id]
  (ensure-started)
  )

(defn recover []
  )

(defn shutdown []
  (cb/close-cupboard!)
  )





