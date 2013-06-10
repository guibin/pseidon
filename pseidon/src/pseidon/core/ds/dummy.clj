(ns pseidon.core.ds.dummy)

(use '[pseidon.core.registry :as r])

(defn dummy-ds [name] 
  (r/->DataSource name (fn [] (prn "Starting " name)) (fn [] (prn "closing " name)) 
                  (fn [] ["resources/plugins/datasources/myds.clj", "resources/plugins/datasources/myds.clj"])
             (fn [file] (clojure.java.io/reader file ))
             )
   )

  

