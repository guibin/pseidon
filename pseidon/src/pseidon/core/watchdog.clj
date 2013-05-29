(ns pseidon.core.watchdog
  (:use clojure.tools.logging)
  )


(defn handle-critical-error [^Throwable t & msg]
  (error t (apply str msg))
  ;any shutdown logic should be handled by JVM shutdown hooks
  (java.lang.System/exit -1)
  )

(defn handle-agent-error [^Throwable e & msg]
  (handle-critical-error e msg)
  )

(defn agent-error-handler [^clojure.lang.Agent agent ^Throwable excp]
  (handle-agent-error excp "Critical error")
  ) 


(defn handle-normal-error [^Throwable e & msg]
  (error e (apply str msg)))


(defn watch-agent-error [f]
  (fn [obj] (try (f obj) (catch Throwable t (handle-agent-error t "agent error")))) 
  )

(defn watch-normal-error [f]
  (fn [& args] (try (apply f args) (catch Throwable t (handle-normal-error t "error")))) 
  )


(defn watch-critical-error [f & arg1]
  (fn [& args] (try (apply f (concat arg1 args)) (catch Throwable t (handle-critical-error t "error")))) 
  )