(ns pseidon.view.metrics
  (:require 
            [pseidon.core.metrics :refer [list-metrics]]
            [pseidon.view.utils :refer [write-json]]
            )
  )

(defn metrics-index [req]
  (write-json (list-metrics)))
