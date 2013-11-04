(ns pseidon.core.app)
(use '[clojure.tools.logging])
(use '[clojure.tools.namespace.repl :only (refresh set-refresh-dirs)])
(use '[pseidon.core.queue :as q])
(use '[pseidon.core.registry :as r])
(use '[pseidon.core.worker :as w])
(use '[pseidon.core.fileresource :as frs])
(use '[pseidon.core.conf :as c])
(use '[pseidon.core.datastore :as ds])
(use '[pseidon.core.tracking :rename  {start tracking-start shutdown tracking-shutdown}])
(use '[pseidon.core.watchdog :as e])
(use '[pseidon.view.server :as view])
(use '[pseidon.core.message :as msg])
(set! *warn-on-reflection* true)

(declare data-queue)

;will reload all of the plugins
(defn refresh-plugins []
  "Reads the plugin-dirs list and if no such files are found uses the default test locations"
   (info (c/get-conf2 :plugin-dirs "NO PLUGIN CONFIG"))
   (apply set-refresh-dirs (c/get-conf2 :plugin-dirs []))
   (binding [*ns* *ns*  clojure.core/*e nil]
	   (refresh)
	   (if clojure.core/*e 
         (e/handle-critical-error clojure.core/*e (str clojure.core/*e) ))
   ))


(defn stop-app []
   (info "Stopping")
   (r/stop-all)
   (frs/close-all)
   (await-for 10000)
   (shutdown-agents)
   (ds/shutdown)
   (tracking-shutdown)
   (shutdown-threads)
   (info "Stopped")
  )

(defn start-app []
  (def data-queue (q/channel "message" :decoder msg/MESSAGE-DECODER))

  (tracking-start)
  (refresh-plugins)
  (Thread/sleep 1000)
  (r/start-all)
  (Thread/sleep 1000)
  (w/start-consume data-queue)  
  (frs/start-services)
  (info "Started")
  (-> (Runtime/getRuntime) (.addShutdownHook  (Thread. (reify Runnable (run [this] (do  
                                                                                     (info "<<< Shutdown from System.exit or kill !!!!  >>>> ")
                                                                                     (try 
                                                                                       (stop-app)
                                                                                       (finally 
                                                                                         (q/close-channel data-queue)))
                                                                                     (info "<<<< Stopped App >>>>")
                                                                                     ))) )))
  (view/start)
  (info "View started")
  )



