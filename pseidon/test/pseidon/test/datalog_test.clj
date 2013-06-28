(ns pseidon.test.datalog_test)
(use '[midje.sweet])
(use '[pseidon.core.conf])
(use '[pseidon.core.datalog])


(defn create-file []
  (let [ f (java.io.File. "target/datalog_test.wal")]
   (.createNewFile f)
   f
  ))

(facts "Test writing and replaying a WAL file"
       
       (def file (create-file))
       

)       