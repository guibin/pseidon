(ns pseidon.core.app)
(use '[clojure.tools.logging])
(use '[clojure.tools.namespace.repl :only (refresh set-refresh-dirs)])
(use '[pseidon.core.queue :as q])
(use '[pseidon.core.registry :as r])
(use '[pseidon.core.worker :as w])

(def data-queue (q/channel))


;will reload all of the plugins
(defn refresh-plugins []
  (set-refresh-dirs (java.io.File. "resources/conf/logging.clj") 
                    (java.io.File. "resources/plugins/ds") (java.io.File. "resources/plugins/channels") 
                    (java.io.File. "resources/plugins/sinks")
                    (java.io.File. "resources/plugins/processors")) (refresh)
  )

(defn start-app []
  (refresh-plugins)
  (Thread/sleep 1000)
  (r/start-all)
  (Thread/sleep 1000)
  (w/start-consume data-queue)  
  (info "Started")
  )

(defn stop-app []
   (r/stop-all)
   (info "Stopped")
  )

