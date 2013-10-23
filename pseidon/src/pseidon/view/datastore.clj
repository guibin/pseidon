(ns pseidon.view.datastore
  (:require [pseidon.core.datastore :refer [list-dirs]]
            [clojure.tools.logging :refer [info ]]
            [pseidon.view.utils :refer [write-json]]))

"A datastore browser that can read and set values, values set set via query strings"

(defn datastore-list [{:keys [query-params]}]
     (let [path (get query-params "path" "/pseidon")]
       (info "using path " path)
       (write-json (list-dirs path))))
  