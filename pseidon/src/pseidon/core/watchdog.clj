(ns pseidon.core.watchdog
  (:use clojure.tools.logging)
  )

(defn handle-agent-error [^Throwable e ^String msg]
  (error e msg))
  

(defn watch-agent-error [f]
  (fn [obj] (try (f obj) (catch Throwable t (handle-agent-error t "agent error")))) 
  )