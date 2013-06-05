(ns pseidon.core.app)
(use '[clojure.tools.logging])
(use '[clojure.tools.namespace.repl :only (refresh set-refresh-dirs)])
(use '[pseidon.core.queue :as q])
(use '[pseidon.core.registry :as r])
(use '[pseidon.core.worker :as w])
(use '[pseidon.core.fileresource :as frs])
(use '[pseidon.core.conf :as c])

(def data-queue (q/channel))

(set! *warn-on-reflection* true)

;will reload all of the plugins
(defn refresh-plugins []
  "Reads the plugin-dirs list and if no such files are found uses the default test locations"
  (apply set-refresh-dirs (c/get-conf2 :plugin-dirs [(java.io.File. "resources/conf/logging.clj") 
                    (java.io.File. "resources/plugins/sinks")
                    (java.io.File. "resources/plugins/datasources") 
                    (java.io.File. "resources/plugins/channels") 
                    (java.io.File. "resources/plugins/processors")] ) )
   (refresh)
  )


(defn stop-app []
   (info "Stopping")
   (r/stop-all)
   (frs/close-all)
   (await-for 10000)
   (shutdown-agents)
   (info "Stopped")
  )

(defn start-app []
  (refresh-plugins)
  (Thread/sleep 1000)
  (r/start-all)
  (Thread/sleep 1000)
  (w/start-consume data-queue)  
  (info "Started")
  (-> (Runtime/getRuntime) (.addShutdownHook  (Thread. (reify Runnable (run [this] (stop-app) )) )))
  
  )



