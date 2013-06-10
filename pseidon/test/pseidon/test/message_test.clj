(ns pseidon.test.message_test)
(use '[midje.sweet])
(use '[pseidon.core.message])


(facts "Test the Message type and supporting methods"
       (fact "Test create message"
        (let [msg (create-message (fn []) "test" true 0 1)]
             (:priority  msg) => 1
             (:topic msg) => "test"
             (:accept msg) => true
             (:ts msg) => 0
             (:priority msg) => 1
        ))
        
       )