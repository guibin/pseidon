(ns pseidon.core.conf
  (:use clojure.tools.logging
        [clojure.edn :only [read-string]]
        ))
;this module contains the application configuration defaults and all logic that is used to read the configuration

(def ^:dynamic *default-conf* "resources/conf/pseidon.edn")

(def conf (ref {}))


(defn load-props[file]
  (read-string (slurp file))
  )
  
(defn load-config! [configFile]
(info "Loading config " configFile)
(dosync (alter conf
               (fn [p] (conj p (load-props configFile)  ))))

)

(defn load-default-config! []
  (load-config! *default-conf*)
  )


(defn get-conf [n]
  (get @conf n)  
  )

(defn get-conf2 [n default-v]
  (get @conf n default-v)  
  )