(ns pseidon.test.core.tracking-test
   (:require [pseidon.core.tracking :refer [create-spec with-txn insert-message! select-ds-messages now status-run query mark-done! deserialize-message ]]
           [pseidon.core.conf :refer [get-conf2] ])
  (:use midje.sweet) 
  (:import 
    (java.io File)
    (org.apache.commons.io FileUtils))
  )


(def new-db-path (fn [] 
               (let [name (str "target/test/tacking-test/" (System/nanoTime))
                     file (File. name)]
                 (if (.exists file)  (FileUtils/deleteDirectory file) )
                 name
               )))

(def new-db-spec (fn [] (create-spec (new-db-path))))

(defn insert-messages [ds n]
  "Inserts n messages into a new database and returns a map with :db-spec :messages [ {:dsid :ts :status } ... ]"
  (let [db (new-db-spec)
        messages (with-txn db
				                 (doall (for [i (range n)] 
				                  (let [msg {:status status-run :dsid (clojure.string/join \u0001 [ds (str i)] ) :ts (now)} ]
				                    (insert-message! msg)
				                    msg
				                  ))))]
    {:db-spec db :messages messages}
    ))

(facts "test insert and select ds messages"
       
       (fact "Test that we can correctly select by datasource messages in status-run state"
              (let [
                    n 100
                    ds "test123"
                    {:keys [db-spec messages]} (insert-messages ds n) 
                    selected-messages (select-ds-messages ds :db db-spec :max n)
                    ]              
                    (count selected-messages) => n
                ))
       
       (fact "Test that we can correctly select by datasource messages in status-run when some messages have been moved to status-done"
              (let [
                    n 100
                    ds "mytest"
                    {:keys [db-spec messages]} (insert-messages ds n) 
                    selected-messages (map deserialize-message (select-ds-messages ds :db db-spec :max n) )
                    
                    ]              
                    (count selected-messages) => n
                    ;mark half the messages as done
                    (mark-done! ds (take (/ n 2) (map :ids selected-messages)) (fn [])  :db db-spec)
                    (count (select-ds-messages ds :db db-spec :max n)) => (/ n 2)
                ))
       
       )

