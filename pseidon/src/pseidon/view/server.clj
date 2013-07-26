(ns pseidon.view.server
  (:require 
    [org.httpkit.server :refer [run-server]]
    [pseidon.core.conf :refer [get-conf2]]
    [compojure.route :refer [files not-found]]
    [compojure.handler :refer [site]]
    [compojure.core :refer [defroutes GET context]]
    [pseidon.view.registry :refer [registry-index]]
    [cheshire.generate :refer [add-encoder]]
    )
  
  )

;add json generators for functions
(add-encoder clojure.lang.IFn 
             (fn [f jsonGenerator]
               (.writeString jsonGenerator "fn")
               ))

(defroutes all-routes
  (GET "/registry" [] registry-index)
  )

(defn start []
  (let [ port (get-conf2 "view.port" 8282)
        ]
    (run-server (site #'all-routes) {:port port}
    )))