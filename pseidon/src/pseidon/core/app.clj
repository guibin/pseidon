(ns pseidon.core.app)
(use '[clojure.tools.namespace.repl :only (refresh set-refresh-dirs)])
(use '[pseidon.core.queue :as q])
(use '[pseidon.core.registry :as r])

(def data-queue (q/channel))


;will reload all of the plugins
(defn refresh-plugins []
  (set-refresh-dirs (java.io.File. "resources/plugins/ds") (java.io.File. "resources/plugins/channels")) (refresh)
  )


(defn start-app [] 
  (refresh-plugins)
  (r/start-channels)  
  )

(defn stop-app []
  (r/stop-channels)
  (r/stop-ds)
  )