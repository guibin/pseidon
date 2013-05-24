(ns pseidon.core.conf
  (:use clojure.tools.logging
        ))
;this module contains the application configuration defaults and all logic that is used to read the configuration

(def ^:dynamic *default-conf* "resources/conf/pseidon.properties")

(def conf (ref {}))


(defn loadProps[file]
  (with-open [rdr (clojure.java.io/reader file)]
    (into {} (doto (java.util.Properties.) (.load rdr)) ))
  )

(defn loadConfig [configFile]
(info "Loading config " configFile)
(dosync (alter conf
               (fn [p] (conj p (loadProps configFile)  ))))

)

(loadConfig *default-conf*)

