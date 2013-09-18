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
    [cheshire.generate :refer [add-encoder]]
    )
   (:import [com.codahale.metrics MetricRegistry Gauge Counter Meter
            Histogram Timer])
  )

;add json generators for functions
(add-encoder clojure.lang.IFn 
             (fn [f jsonGenerator]
               (.writeString jsonGenerator "fn")
               ))


(def map-encoder 
	     (fn [m jsonGenerator]
		(.writeString jsonGenerator (map str m))
		))

(add-encoder clojure.lang.PersistentArrayMap map-encoder)
(add-encoder java.util.Map map-encoder)
(add-encoder clojure.lang.PersistentHashMap map-encoder)


(add-encoder Gauge 
             (fn [m jsonGenerator]
              (.writeObject jsonGenerator {:value (.getValue m)})))

(add-encoder Timer
             (fn [m jsonGenerator]
               (let [s (.getSnapshot m)]
		(.writeObject jsonGenerator {:count (.getCount m) :mean-rate (.getMeanRate m) 
					     :one-minute-rate (.getOneMinuteRate m) 
					     :five-minute-rate (.getFiveMinuteRate m) 
					     :fifteen-minute-rate (.getFifteenMinuteRate m) 
					     :median (.getMedian s) 
                                             :max (.getMax s) 
                                             :min (.getMin s) 
                                             :mean (.getMean s) 
                                             :std-dev (.getStdDev s)} )
             )))

(add-encoder Counter
             (fn [m jsonGenerator] (.writeObject jsonGenerator {:counter (.getCounter m)})))

(add-encoder Meter
             (fn [m jsonGenerator]
		(.writeObject jsonGenerator {:count (.getCount m) :mean-rate (.getMeanRate m) :one-minute-rate (.getOneMinuteRate m) 
					     :five-minute-rate (.getFiveMinuteRate m) :fifteen-minute-rate (.getFifteenMinuteRate m) }    )  ))

(add-encoder Histogram
             (fn [m jsonGenerator]
               (let [s (.getSnapshot m)]
	                   (.writeObject jsonGenerator { :count (.getCount m) 
					    :median (.getMedian s)
					    :max (.getMax s)
					    :min (.getMin s)
					    :mean (.getMean s)
					    :std-dev (.getStdDev s) } ))
             ))



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
