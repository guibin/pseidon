(ns pseidon.view.utils
  (:import  [org.codehaus.jackson.map ObjectMapper])
  )

(def objMapper (ObjectMapper.))

(defn write-json [obj]
  (let [writer (java.io.StringWriter.)]
    (.writeValue objMapper writer obj)
    (.toString writer)))

(defn str->int [s]
  (if-not s 0 (Integer/parseInt s)))
