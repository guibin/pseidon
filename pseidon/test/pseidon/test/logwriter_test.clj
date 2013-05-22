(ns pseidon.test.logwriter_test)
(use '[midje.sweet])
(use '[pseidon.core.sink.logwriter])

(facts "Test the LogWriter functionality"
       (fact "Get Topic config"
            (.-topic (get-topic-config "test")) => "test" 
             ))