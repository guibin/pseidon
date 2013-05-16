(ns pseidon.core.ds.dummy)

(use '[pseidon.core.registry :as r])

(defn dummy-ds [name] 
  (r/->DataSource name (fn [] (prn "closing " name)) 
                  (fn [] ["resources/plugins/ds/myds.clj", "resources/plugins/ds/myds.clj"])
             (fn [file] (clojure.java.io/reader file ))
             )
   )

  

