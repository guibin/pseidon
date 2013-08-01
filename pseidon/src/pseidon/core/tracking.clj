(ns pseidon.core.tracking
 (:require [clojure.java.jdbc :as sql]
           [pseidon.core.conf :refer [get-conf2] ]
           [pseidon.core.utils :refer [buffered-select]]
           )
  )

(def status-run "run")
(def status-done "done")

(defn now [] (java.util.Date.))


(defn tracking-start [] )

(def ^:Dynamic dbspec)

(defn ^:Dynamic ensure-started [] )

(defmacro txn-helper [spec & body]
	      `(clojure.java.jdbc/with-connection
	         ~spec
           (clojure.java.jdbc/transaction ~@body
	         )))

(defmacro with-txn 
  ([body]
    "Runs body inside a db transaction and connection"
   `(txn-helper dbspec ~body))
  ([spec body]
    `(txn-helper ~(if-not spec dbspec spec) ~body)))

(defn as-str [& s] (apply str s))

(defn create-query-paging [{:keys [tbl properties predicate from max] :or {max 100} }]
  "Creates a SQL query using paging and ROWNUM()"
  (str "SELECT * from (select " (clojure.string/join "," (map #(str "a." %) properties)) 
                        ", ROWNUM() rnum from (select " (clojure.string/join "/" properties) 
                        " from " tbl
                        " order by dsid,ts,status) a "
                        " WHERE ROWNUM() <= " (+ from max)
                        ") WHERE " (if-not predicate "" (str predicate " and ")) " rnum >= " from))

(defn create-table[]
    "Creates the message table, if it already exists the method silently fails."
		(defn create-tables
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
    
      (wrap-table-exist-exception create-tables))

(defn create-spec [path]
   (let [spec  {:classname "org.hsqldb.jdbcDriver" 
     :subprotocol "hsqldb" 
     :subname (str "file:"  path ";create=true")
     :user "sa"
     :password "sa"
     }]
     (with-txn spec (create-table))
     spec
     ))


(def dbspec (create-spec (get-conf2 "tracking-db-dir" "/tmp/pseidon-tracking4")))

(defn query [q max]
   (sql/with-query-results rs [q] 
     (let [dsid (:dsid (first rs) )]
       (vec (take max rs))))
   )

(defn get-message [dsid]
  (first (query (str "select * from messagetracking where dsid='" dsid "'") 1)))
  

(defn delete-message! [dsid]
    (sql/delete-rows :messagetracking ["dsid=?" dsid]))
  
(defn insert-message! [{:keys [dsid status ts]}]
  "Insert data into the table"
  [dsid status ts]
  (clojure.java.jdbc/insert-values
   :messagetracking
   [:dsid :status :ts]
   [dsid status ts]))




(defn mark-run! [^String ds ^String id & {:keys [db]}]
  " The ds and id values cannot hold any byte 1 characters, the key formed is ds byte1 id and must be unique, 
    if the object already exists in the database a unique constraint exception will be thrown.

    This method saves the message tracking metadata with status==run
  "
  (with-txn db
	  (let [ds-id (clojure.string/join \u0001 [ds id] )]
	      (insert-message! {:dsid ds-id :status status-run :ts (now)})    
	    )))

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

(defn mark-done! [^String ds ids ^clojure.lang.IFn f & {:keys [db]}]
  "
    Applies the function f inside a transaction together with the set status to done
    If f fails the status will be rolled back.
    The status is set first to ensure that if there is any failure with the emebedded db the function f is never applied.
  "
    (with-txn db
      ;if ids is a sequence update every id and then apply f
      (doseq [id (if (sequential? ids) ids [ids])
              ds-id (clojure.string/join \u0001 [ds id] )
              ]
				      (update-message!  
				            ds-id
					          {:status status-done}))
          )
				  (f))


    
(defn select-run-messages [^String ds & {:keys [max] :or {:max 100} } ]
  "Creates a lazy sequence of messages for this datasource"
  (letfn [ (m-select [from] (select-messages (str "dsid like '" ds \u0001 "'") from max)) ]
    (buffered-select m-select 0)))

(defn shutdown []
  )





