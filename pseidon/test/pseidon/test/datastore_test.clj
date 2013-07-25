(ns pseidon.test.datastore_test)
(use '[midje.sweet])
(use '[pseidon.core.conf])
(use '[pseidon.core.datastore])


(def zk-server (org.apache.curator.test.TestingServer.))
(def zk-url (str "localhost:" (.getPort zk-server)))


(facts "Test that we can save and retreive information"
       (load-default-config!)
       (set-conf! :zk-url zk-url)

       (fact "Set/Get"
             (set-data! "/package" "testfile" 123) => 123
             (do (set-data! "/package" "testfile" 123)  (get-data-number "/package" "testfile")) => 123)
       (comment
       (fact "Inc/Get"
             (set-data! :package :testfile2 (long 123) ) => 123
             (do (set-data! :package :testfile2 (long 123) ) (inc-data! :package :testfile2 12) => 135
             (dec-data! :package :testfile2 1) => 134)
       )
       
       ))


(facts "Test directory listing"
       (fact "List children"
             (do (mkdirs "/a" "b" "c") (mkdirs "/a" "b" "d") (mkdirs "/a" "b" "e") (list-dirs "/a" "b")) => ["c" "d" "e"]
             )
       )
       