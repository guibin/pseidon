(ns pseidon.core.utils
  )

(defn buffered-select [f-select init-pos]
  "Creates a lazy sequence of messages for this datasource"
  (letfn [  
           (m-seq [buff pos] 
                   (let [buff2 (if (empty? buff) (f-select pos) buff)]
                         (cons (first buff2) (lazy-seq (m-seq (rest buff2) (inc pos) )))         
                     )
                   )
           ]
    (m-seq nil init-pos)
    )
  )