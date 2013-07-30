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

(defmacro with-txn [& body]
  "Runs body inside a db transaction and connection"
 `(clojure.java.jdbc/with-connection
   dbspec
   (clojure.java.jdbc/transaction ~@body)))

(defn as-str [& s] (apply str s))

(defn create-query-paging [{:keys [tbl properties predicate from max] :or {max 100} }]
  "Creates a SQL query using paging and ROWNUM()"
  (str "SELECT * from (select " (clojure.string/join "," (map #(str "a." %) properties)) 
                        ", ROWNUM() rnum from (select " (clojure.string/join "/" properties) 
                        " from " tbl (if-not predicate "" (as-str " where " predicate))
                        " order by ts, dsid, status ) a "
                        " WHERE ROWNUM() <= " max
                        ") WHERE rnum >= " from))

(defn create-table[]
    "Creates the message table, if it already exists the method silently fails."
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
    
     (with-txn (wrap-table-exist-exception create-tables)))

(defn query [q max]
 (sql/with-connection pseidon.core.tracking/dbspec
   (sql/with-query-results rs [q] (vec (take max rs)))))

(defn get-message [dsid]
  (first (query (str "select * from messagetracking where dsid=" dsid) 1)))
  

(defn delete-message [dsid]
  (sql/with-connection dbspec
    (sql/delete-rows :messagetracking ["dsid=?" dsid])))
  
  
(defn insert-message! [{:keys [dsid status ts]}]
  "Insert data into the table"
  [dsid status ts]
  (clojure.java.jdbc/insert-values
   :messagetracking
   [:dsid :status :ts]
   [dsid status ts]))


(defn start []
  (def pseidon.core.tracking/dbspec 
    {:classname "org.hsqldb.jdbcDriver" 
     :subprotocol "hsqldb" 
     :subname (str "file:"  (get-conf2 "tracking-db-dir" "/tmp/pseidon-tracking4") ";create=true")
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
      (insert-message! {:dsid ds-id :status status-run :ts (now)})    
	    ;(cb/make-instance messagetracking [(System/currentTimeMillis) ds-id status-run])
    )
  )

;(defn create-query-paging [{:keys [tbl properties predicate from max]}]
  
(defn select-messages  
  "Returns a vector of messages from to max"
  ([from max]
  (query (create-query-paging {:tbl "messagetracking" :properties ["*"] :from from :max max} ) (+ from max)))
  ([where from max]
    (query (create-query-paging {:tbl "messagetracking" :predicate where :properties ["*"] :from from :max max} ) (+ from max))))

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





