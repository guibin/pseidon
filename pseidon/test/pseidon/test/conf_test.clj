(ns pseidon.test.conf_test)
(use '[midje.sweet])
(use '[pseidon.core.conf])

(facts "Test that configuration can be read"
       
       (fact "Load configuration"
             (get @conf "name") => "server1")
             )
       
      
