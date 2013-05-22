(ns pseidon.test.registry_test)
(use '[midje.sweet])
(use '[pseidon.core.registry])


(defn registerTestDS []
  (register (->DataSource "ds-testreg" (fn [] "start") (fn [] (prn "stop")) (fn [] ["1"]) (fn [] "reader")))
  (reg-get "ds-testreg")
  )

(facts "Test the registry"
       
       (fact "Test that we can register a DS and retreive it"
             (:name (registerTestDS)) => "ds-testreg"  
             )
       
       )

