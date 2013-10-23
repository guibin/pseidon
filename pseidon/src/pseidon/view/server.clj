(ns pseidon.view.server
  (:require 
    [org.httpkit.server :refer [run-server]]
    [pseidon.core.conf :refer [get-conf2]]
    [compojure.route :refer [files not-found]]
    [compojure.handler :refer [site]]
    [compojure.core :refer [defroutes GET context]]
    [pseidon.view.datastore :refer [datastore-list]]
    [pseidon.view.registry :refer [registry-index]]
    [pseidon.view.tracking :refer [tracking-index]]
    [pseidon.view.metrics  :refer [metrics-index]]
    [pseidon.view.home :refer [home-index]]
    )
    (:use clojure.tools.logging)
  )


(defroutes all-routes
  (GET "/" [] (fn [req] (home-index req)))
  (GET "/registry" [] registry-index)
  (GET "/tracking" [] (fn [req] (tracking-index req) ))
  (GET "/metrics"  [] (fn [req] (metrics-index req)))
  (GET "/datastore" [] (fn [req] (info "req " req) (datastore-list req)))
  )

(defn start []
  (let [ port (get-conf2 "view.port" 8282)
        ]
    (run-server (site #'all-routes) {:port port}
    )))
