(ns pseidon.view.server
  (:require 
    [org.httpkit.server :refer [run-server]]
    [pseidon.core.conf :refer [get-conf2]]
    [compojure.route :refer [files not-found]]
    [compojure.handler :refer [site]]
    [compojure.core :refer [defroutes GET context]]
    [pseidon.view.registry :refer [registry-index]]
    [pseidon.view.tracking :refer [tracking-index]]
    [pseidon.view.metrics  :refer [metrics-index]]
    )
    
  )


(defroutes all-routes
  (GET "/registry" [] registry-index)
  (GET "/tracking" [] (fn [req] (tracking-index req) ))
  (GET "/metrics"  [] (fn [req] (metrics-index req)))
  )

(defn start []
  (let [ port (get-conf2 "view.port" 8282)
        ]
    (run-server (site #'all-routes) {:port port}
    )))
