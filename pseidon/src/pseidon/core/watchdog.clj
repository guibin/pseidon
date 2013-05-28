(ns pseidon.core.watchdog
  (:use clojure.tools.logging)
  )

(defn handle-agent-error [^Throwable e ^String msg]
  (error e msg))

(defn agent-error-handler [^clojure.lang.Agent agent ^Throwable excp]
  (handle-agent-error excp "Critical error")
  
  ) 
  
(defn handle-critical-error []
  (shutdown-agents)
  (java.lang.System/exit -1)
  )

(defn watch-agent-error [f]
  (fn [obj] (try (f obj) (catch Throwable t (handle-agent-error t "agent error")))) 
  )