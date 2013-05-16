(ns pseidon.core.app)
(use '[clojure.tools.namespace.repl :only (refresh set-refresh-dirs)])
(use '[pseidon.core.queue :as q])

(def data-queue (q/channel))

;will reload all of the plugins
(defn refresh-plugins []
  (set-refresh-dirs (java.io.File. "resources/plugins/ds")) (refresh)
  (set-refresh-dirs (java.io.File. "resources/plugins/channels")) (refresh)
  
  )

