(ns pseidon.view.registry
  (:require 
            [pseidon.core.registry :refer [reg-list-all]]
            [cheshire.core :refer :all]
            )
  )


(defn registry-index [req]
  (generate-string (reg-list-all))
  )