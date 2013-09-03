(ns pseidon.core
  (:gen-class)
  (:use [clojure.tools.cli :only [cli]] 
        [pseidon.core.conf :only [load-config! get-conf2] ]
        [pseidon.core.app :only [start-app stop-app refresh-plugins]]
        [clojure.tools.nrepl.server :only [start-server stop-server] ]
        ) 
  )

 (require '[clojure.tools.nrepl :as repl])
 
(def repl-server (ref nil))

(defn cmd [args] 
  
  (cli args
    ["-c" "--config" "Configuration directory that must contain a pseidon.edn file" :parse-fn #(java.io.File. %) :default (java.io.File. "/opt/pseidon/conf")]
    ["-stop" "--stop" "Shutdown the application" :flag true]
    ["-r" "--refresh-plugins" "Refresh all edited plugins" :flag true]
    ["-get-cp" "--get-cp" "Returns classpath string from the pseidon.edn classpath setting"]
  ))



(defn parse-cp-item [item]
  (if (and  
         (-> (clojure.java.io/file item) .exists)
         (not (.endsWith item "*"))
       )
    (str item "/*:" item)
    item    
    ))

(defn get-classpath [] 
   (clojure.string/join ":" (map parse-cp-item (get-conf2 :classpath ["/opt/pseidon/lib/*"]))))

(defn check-opts2 [opts]
 "Check that all the options required are defined, else print the usage"
  (if-let [conf (:config opts)]
    (if-not (cond (.exists conf) (complement (.isFile conf)) )
     (do (println "The configuration directory " conf " is either not a directory or does not exist"))  
     opts
    ))
  
  )

(defn check-opts [[opts _ usage] ]
  (if-let [m (check-opts2 opts)] m (do (println usage) false)) 
  )


(defn start-repl [repl-port]
   (dosync (ref-set repl-server (start-server :port repl-port)) )
  )

(defn shutdown[]
  (stop-app)
  )

(defn app-refresh-plugins[]
  (refresh-plugins)
  )


(defn send-refresh []
  "we need to connect to the repl"
   (with-open [conn (repl/connect :port (get-conf2 :repl-port 7111))]
      (-> (repl/client conn 1000) (repl/message {:op :eval :code "(do (pseidon.core/app-refresh-plugins) )"  })
          (repl/response-values)
                                  )))

(defn send-shutdown []
  "we need to connect to the repl"
   (with-open [conn (repl/connect :port (get-conf2 :repl-port 7111))]
      (-> (repl/client conn 1000) (repl/message {:op :eval :code "(do (pseidon.core/shutdown) )"  })
          (repl/response-values)
                                  )))

(defn -main [& args]
     
     (if-let [opts (check-opts (cmd args) ) ]
       (do
          (load-config! (clojure.string/join "/" [ (:config opts) "pseidon.edn"] )) 
          
		      (cond
		              (:refresh-plugins opts) (send-refresh)
		             (:stop opts) (send-shutdown)
		             (contains? opts :get-cp) (.println System/out (get-classpath))
		              :else
		               (do 
				             (start-repl (get-conf2 :repl-port 7111))
				             (start-app))             
		          ))))

