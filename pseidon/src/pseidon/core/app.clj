(ns pseidon.core.app)
(use '[clojure.tools.logging])
(use '[clojure.tools.namespace.repl :only (refresh set-refresh-dirs)])
(use '[pseidon.core.queue :as q])
(use '[pseidon.core.registry :as r])
(use '[pseidon.core.worker :as w])
(use '[pseidon.core.fileresource :as frs])
(use '[pseidon.core.conf :as c])
(use '[pseidon.core.datastore :as ds])

(set! *warn-on-reflection* true)

(def data-queue (q/channel))


;will reload all of the plugins
(defn refresh-plugins []
  "Reads the plugin-dirs list and if no such files are found uses the default test locations"
   (info (c/get-conf2 :plugin-dirs "NO PLUGIN CONFIG"))
   (apply set-refresh-dirs (c/get-conf2 :plugin-dirs []))
   (binding [*ns* *ns*]
   (refresh))
  )


(defn stop-app []
   (info "Stopping")
   (r/stop-all)
   (frs/close-all)
   (await-for 10000)
   (shutdown-agents)
   (ds/shutdown)
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



