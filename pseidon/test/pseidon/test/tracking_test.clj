(ns pseidon.test.tracking_test
  
  (:use midje.sweet
        pseidon.core.tracking
         ))

(defn get-dbspec [p]
  (let [p1 (str p "/" (System/currentTimeMillis))
        file (java.io.File. p1)]
    (.mkdirs file)
    (create-spec (str p1 "/mydb"))
    ))

(facts "Test tracking methods"
       (fact "Test select group by"
             (let [ db (get-dbspec "target/tracking_test/mydb1" )
                    dsid-pref (str (System/currentTimeMillis) )
                   ]
               
               ;insert-message! [{:keys [dsid status ts]
               (with-txn db 
                 (do 
                 (insert-message! {:dsid (str dsid-pref "-1") :status "ready" :ts (now)})
                 (insert-message! {:dsid (str dsid-pref "-2") :status "ready" :ts (now)})
                 (insert-message! {:dsid (str dsid-pref "-3") :status "done" :ts (now)})
                 )
                 )
               (prn "Status count " (message-statuscount "where dsid" :db db) )
               
               (message-statuscount "where dsid" :db db) => [{:n 2, :status "ready"} {:n 1, :status "done"}]
               
               )
             )
       
       )