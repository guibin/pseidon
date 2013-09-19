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
            Histogram Timer]
            [org.codehaus.jackson.map ObjectMapper] )
  )

(def objMapper (ObjectMapper.))

(defn write-json [obj]
  (let [write (java.io.StringWriter.)]
    (.writeValue objMapper write obj)
    (.toString write)))

;add json generators for functions
(add-encoder clojure.lang.IFn 
             (fn [f jsonGenerator]
               (.writeString jsonGenerator "fn")
               ))


(def obj-encoder 
	     (fn [m jsonGenerator]
		(.writeString jsonGenerator (write-json m))
		))

(add-encoder clojure.lang.PersistentArrayMap obj-encoder)
(add-encoder java.util.Map obj-encoder)
(add-encoder clojure.lang.PersistentHashMap obj-encoder)


(add-encoder Gauge 
             (fn [m jsonGenerator]
              (obj-encoder {:value (.getValue m)} jsonGenerator)))
              
(add-encoder Timer
             (fn [m jsonGenerator]
               (let [s (.getSnapshot m)]
		(obj-encoder {:count (.getCount m) :mean-rate (.getMeanRate m) 
					     :one-minute-rate (.getOneMinuteRate m) 
					     :five-minute-rate (.getFiveMinuteRate m) 
					     :fifteen-minute-rate (.getFifteenMinuteRate m) 
					     :median (.getMedian s) 
                                             :max (.getMax s) 
                                             :min (.getMin s) 
                                             :mean (.getMean s) 
                                             :std-dev (.getStdDev s)} jsonGenerator)
             )))

(add-encoder Counter
             (fn [m jsonGenerator] (obj-encoder {:counter (.getCounter m)} jsonGenerator)))

(add-encoder Meter
             (fn [m jsonGenerator]
		(obj-encoder {:count (.getCount m) :mean-rate (.getMeanRate m) :one-minute-rate (.getOneMinuteRate m) 
					     :five-minute-rate (.getFiveMinuteRate m) :fifteen-minute-rate (.getFifteenMinuteRate m) } jsonGenerator)  ))

(add-encoder Histogram
             (fn [m jsonGenerator]
               (let [s (.getSnapshot m)]
	                   (obj-encoder { :count (.getCount m) 
					    :median (.getMedian s)
					    :max (.getMax s)
					    :min (.getMin s)
					    :mean (.getMean s)
					    :std-dev (.getStdDev s) } jsonGenerator))
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
