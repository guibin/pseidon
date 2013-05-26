(ns plugins.sinks.hdfs
  (:use pseidon.core.fileresource
        pseidon.core.registry)
  )

;[name start stop writer]

(register (->DataSink "ds-writer" (fn [] ) (fn [] (close-all)) (fn [msg] )))