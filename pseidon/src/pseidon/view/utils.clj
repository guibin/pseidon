(ns pseidon.view.utils
  )

(defn str->int [s]
  (if-not s 0 (Integer/parseInt s)))

