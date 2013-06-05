(ns pseidon.core
  (:use [clojure.tools.cli :only [cli]] 
        [pseidon.core.conf :only [load-config! get-conf2] ]
        [pseidon.core.app :only [start-app stop-app]]
        [clojure.tools.nrepl.server :only [start-server stop-server] ]
        ) 
  )

 (require '[clojure.tools.nrepl :as repl])
 
(def repl-server (ref nil))

(defn cmd [args] 
  
  (cli args
    ["-c" "--config" "Configuration directory that must contain a pseidon.edn file" :parse-fn #(java.io.File. %) :default (java.io.File. "/opt/pseidon/conf")]
    ["-stop" "--stop" "Shutdown the application" :flag true]
  ))



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

(defn send-shutdown []
  "we need to connect to the repl"
   (with-open [conn (repl/connect :port (get-conf2 :repl-port 7111))]
      (-> (repl/client conn 1000) (repl/message {:op :eval :code "(do (pseidon.core/shutdown) )"  })
          (repl/response-values)
                                  )))

(defn main [& args]

     (if-let [opts (check-opts (cmd args) ) ]
       (if (:stop opts) 
         (send-shutdown)
         (do 
             (load-config! (clojure.string/join "/" [ (:config opts) "pseidon.edn"] )) 
             (start-repl (get-conf2 :repl-port 7111))
             (start-app)
           
             ))
       ))


