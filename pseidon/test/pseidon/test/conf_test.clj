(ns pseidon.test.conf_test)
(use '[midje.sweet])
(use '[pseidon.core.conf])

(facts "Test that configuration can be read"
       (load-default-config!)

      ; (fact "Load configuration"
       ;      (get-conf :name) => "server1")
       ;(fact "Load configuration default values"
        ;     (get-conf2 :abc12345567 "server10") => "server10")
       )
       
      
