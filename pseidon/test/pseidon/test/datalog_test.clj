(ns pseidon.test.datalog_test)
(use '[midje.sweet])
(use '[pseidon.core.conf])
(use '[pseidon.core.datalog])


(defn write-testdata [n file-name]
  (let [wal-file (create-walfile file-name)]
   (doseq [m (range n)]
     (wal-write wal-file (byte-array (map #(byte %) (range 10))) )
     )
   (close wal-file)
  ))

(facts "Test writing and replaying a WAL file"
   (fact "test write and replay"  
    (let  [file-name (str "target/myfile." (System/currentTimeMillis) ".bin-wal")
                ]
          (write-testdata 100 file-name)
          (count (wal-seq file-name)) => 100
          (destroy file-name)
        )
   ))       