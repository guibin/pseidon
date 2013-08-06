(ns pseidon.test.utils_test
  (require 
    [pseidon.core.utils :refer :all]
    [midje.sweet :refer :all]
    )
  )

(fact "Test vects"
     
      (merge-distinct-vects [1 2 3] [1 2 3 4 5]) => [1 2 3 4 5]
      
       )