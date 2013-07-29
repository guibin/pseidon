(ns pseidon.core.tracking
 (:require [clojure.java.jdbc :as sql]
           [pseidon.core.conf :refer [get-conf2] ]
           )
  )

(def status-run "run")
(def status-done "done")

(def started? (java.util.concurrent.atomic.AtomicBoolean.))

(defn now [] (java.util.Date.))

(def dbspec)

(defn with-txn [f]
 (clojure.java.jdbc/with-connection
   dbspec
   (clojure.java.jdbc/transaction (f))))

(defn create-table[]
		(defn create-tables
		  "Create a factoid table"
		  []
		  (do 
		    (sql/create-table
		      "messagetracking"
		      [:dsid "VARCHAR(255)" "NOT NULL" "PRIMARY KEY"]
		      [:status "VARCHAR(20)" "NOT NULL"]
		      [:ts "TIMESTAMP" "NOT NULL"])
		   (sql/do-commands "CREATE INDEX messagetraking_index1 ON messagetracking(dsid, status, ts)")))
		
	   (defn wrap-table-exist-exception [f]
	    (try (f) 
	        (catch java.sql.BatchUpdateException e 
	               (if-not (re-find #"name already exists" (.toString e)) (throw e)))))
     (with-txn #(wrap-table-exist-exception create-tables)))

(defn query [q]
 (sql/with-connection dbspec
   (sql/with-query-results rs [q] rs)))


(defn insert-message! [{:keys [dsid status ts]}]
  "Insert data into the table"
  [dsid status ts]
  (clojure.java.jdbc/insert-values
   :messagetracking
   [:dsid :status :ts]
   [dsid status ts]))


(defn start []
  (cb/open-cupboard! (get-conf2 "tracking-db-dir" "/tmp/pseidon-tracking"))
  (def dbspec 
    {:classname "org.hsqldb.jdbcDriver" 
     :subprotocol "hsqldb" 
     :subname "file:" (get-conf2 "tracking-db-dir" "/tmp/pseidon-tracking") ";create=true"
     :user "sa"
     :password "sa"
    })
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
      (insert-message! {:dsid ds-id :status status-run :ts now})    
	    ;(cb/make-instance messagetracking [(System/currentTimeMillis) ds-id status-run])
    )
  )

(defn ls-tracking [from to]  
  )

(defn apply-in-txn [fn-seq]  
  " Sequence of functions to be applied in a single transaction
  "
  (ensure-started)
  (with-txn [:no-sync false]
    (doseq [f fn-seq] (f) )
    ))
 

(defn update-message! [dsid params]
  "This method updates a blog entry"
  [dsid params]
  (clojure.java.jdbc/update-values
   :messagetracking
   ["dsid=?" dsid]
   params))

(defn mark-done! [^String ds ids ^clojure.lang.IFn f]
  "
    Applies the function f inside a transaction together with the set status to done
    If f fails the status will be rolled back.
    The status is set first to ensure that if there is any failure with the emebedded db the function f is never applied.
  "
  (ensure-started)
    (with-txn [:no-sync false] 
      ;if ids is a sequence update every id and then apply f
      (doseq [id (if (sequential? ids) ids [ids])
              ds-id (clojure.string/join \u0001 [ds id] )
              ]
				      (update-message!  
				            ds-id
					          {:status status-done}))
          )
				  (f))
    
(defn recover []
  )

(defn shutdown []
  )





