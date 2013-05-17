(ns pseidon.core.conf)
;this module contains the application configuration defaults and all logic that is used to read the configuration


(def ^@dynamic *default-conf* "resources/conf/pseidon.properties")

(def conf (ref {}))


(defn loadProps[file]
  (with-open [rdr (clojure.java.io/reader file)]
    (into {} (doto (java.util.Properties.) (.load rdr)) ))
  )

(dosync (alter conf
               (fn [p] (conj p (loadProps *default-conf*)  ))))

