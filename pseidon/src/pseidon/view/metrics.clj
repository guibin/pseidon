(ns pseidon.view.metrics
  (:require 
            [pseidon.core.metrics :refer [list-metrics]]
            [cheshire.core :refer :all]
            )
  )


(defn metrics-index [req]
  (generate-string (list-metrics)))
