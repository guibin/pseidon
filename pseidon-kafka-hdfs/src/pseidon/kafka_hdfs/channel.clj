(ns pseidon.kafka-hdfs.channel
  (:require [pseidon.core.conf :refer [get-conf2]]
            [pseidon.core.queue :refer [publish]]
            [pseidon.core.message :refer [create-message]]
            [pseidon.core.metrics :refer [add-meter update-meter] ]
            [pseidon.core.registry :refer [register ->Channel reg-get-wait] ]
            [pseidon.core.watchdog :refer [watch-critical-error]]
            [pseidon.core.tracking :refer [select-ds-messages mark-run! mark-done! deserialize-message]]
     	      [clojure.tools.logging :refer [info error]]
            [taoensso.nippy :as nippy]
            [clj-json.core :as json]
            [pseidon.kafka-hdfs.json-csv :as json-csv]
            [clj-time.coerce :refer [from-long to-long]]
            [clj-json [core :as json]]
            [clj-time.format :refer [unparse formatter]]
            [clojure.java.jdbc :as sql]
            [fun-utils.core :refer [fixdelay]]
            [org.tobereplaced.jdbc-pool :refer [pool]])
  (:import (java.util.concurrent Executors TimeUnit)
           (java.util.concurrent.atomic AtomicBoolean)
           [org.apache.commons.lang StringUtils]
           [java.io DataOutputStream]
           [net.minidev.json JSONValue]))

  
(defmacro with-info [s body]
  `(let [x# ~body] (info ~s " " x#) x#))

(defn json-csv-encoder 
  "Data should be the data returned from json-csv/parse-definitions"
  [msg & data]
  (json-csv/json->csv data "," msg))

(defn ^"[B" default-encoder [msg & _]
  msg)


(defn nippy-encoder [msg]
  (nippy/freeze msg))

(defn json-encoder [msg & _]
  (let [^java.io.ByteArrayOutputStream bts-array (java.io.ByteArrayOutputStream. (* 2 (count msg)))]
    (with-open [^java.io.OutputStreamWriter appendable (java.io.OutputStreamWriter. bts-array)]
       (net.minidev.json.JSONValue/writeJSONString msg appendable))
    (.toByteArray bts-array)))
      


(defonce ^:constant encoder-map {:default default-encoder
                                 :nippy nippy-encoder
                                 :json json-encoder
                                 :json-csv json-csv-encoder})

(def get-fixed-encoder (memoize (fn [log-name]
                            (with-info (str "For topic " log-name " using encoder ")
				                            (let [key1 (keyword (str "kafka-hdfs-" log-name "-encoder"))
				                                  key2 :kafka-hdfs-default-encoder]
				                              (if-let [encoder (get encoder-map (get-conf2 key1 (get-conf2 key2 :default))) ]
				                                encoder
				                                default-encoder))))))

(defn get-db-connecion []
   {:classname (get-conf2 :etl-tracking-driver "com.mysql.jdbc.Driver")
                :subprotocol (get-conf2 :hdfs-tracking-subprotocol "mysql") 
                :subname     (get-conf2 :hdfs-tracking-subname "//localhost:3306/db") 
                :user        (get-conf2 :hdfs-tracking-user "root")
                :password    (get-conf2 :hdfs-tracking-pwd "")})

(defonce DEFAULT_DB (delay (pool 
                         (get-db-connecion)
                         :max-statements 100
                         :max-pool-size 1)))


(defn load-topics 
  "load topics from the kafka-logs table"
  [host & {:keys [db] :or {db (force DEFAULT_DB)}}]
  (sql/with-connection db
    (sql/with-query-results rs [(str "select log from kafka_logs where type='hdfs' and host='" host "' and enabled=1")] (vec (map :log rs)))))

(defn load-parser-data 
  "load topics from the kafka-logs table"
  [& {:keys [db] :or {db (force DEFAULT_DB)}}]
  (sql/with-connection db
    (vec (sql/with-query-results rs [(str "select log,type,data from kafka_log_encoders")] ))))


(def ch-dsid "pseidon.kafka-hdfs.channel")
(defonce service (Executors/newSingleThreadExecutor))
(defonce host-name (-> (java.net.InetAddress/getLocalHost) .getHostName))

(def kafka-reader  (delay (let [{:keys [reader-seq]} (reg-get-wait "pseidon.kafka.util.datasource" 10000)] reader-seq)))

(defonce topic-parsers (ref {}))


(defn get-encoder [log-name]
  (if-let [encoder (get @topic-parsers log-name)]
    encoder
    (get-fixed-encoder log-name)))

(defn parse-parser-def 
  "Returns a function that acceps a single argument"
  [t data]
  (let [k-t (keyword t)
        json-data (json/parse-string data)
        encoder (get encoder-map k-t (get encoder-map :default))]
    (cond 
      (= t :json-csv)
      (let [parsed-def (json-csv/parse-definitions json-data)]
        #(apply encoder %1 parsed-def)
      :else
      #(apply encoder %1 json-data)))))
        
(defn update-topic-parsers [topic-data]
  (dosync 
    (alter topic-parsers 
      (fn [m] 
        (try
          (reduce (fn [state {:keys [log type data]}]
                     (assoc state log (parse-parser-def type data)) state) {} topic-data)
          (catch Exception e (do (error e e) m)))))))            

(defn process-topics [topics]
  "Start in infinite loop that will read the current batch ids and send to the hdfs topic
  "
  (let [consume-meter-map (into {} (map (fn [n] [n (add-meter (str "pseidon.kafka_hdfs.channel-" n))]) topics))
        topics-ref (ref (set topics))
        {:keys [reader-seq add-topic remove-topic]}(reg-get-wait "pseidon.kafka.util.datasource" 10000)]
    
	  (fixdelay 10000
	    (try
	      (let [
             _ (update-topic-parsers (load-parser-data ))
             logs (set (load-topics host-name))
             logs-to-add (clojure.set/difference logs @topics-ref)
             logs-to-remove (clojure.set/difference @topics-ref logs)]
         (info "logs " logs " logs-to-add " logs-to-add " logs-to-remove " logs-to-remove " topics-ref " topics-ref)
         (doseq [log logs-to-add]
           (add-topic log))
         (doseq [log logs-to-remove]
           (remove-topic log))
         (dosync 
           (if (not (empty? logs-to-add))
            (alter topics-ref #(apply conj % logs-to-add)))
           (if (not (empty? logs-to-remove))
            (alter topics-ref #(apply disj % logs-to-remove)))))
         (catch Exception e (error e e))))
	  
        
		  (while (not (Thread/interrupted))
		    (let [rdr-seq (apply reader-seq topics)]
	        (doseq [msgs rdr-seq]
		        (let [msg-id 1]
		          (try
	             (do
	               (publish "pseidon.kafka-hdfs.processor" (create-message msgs ch-dsid msg-id ch-dsid true -1 1)))
	             (catch java.sql.BatchUpdateException e (info "ignore duplicate message " msg-id)))))))))


(defn ^:dynamic channel-init []
  
  ;this will be called when any file written by this channel has been rolled
  ;we send the rolled file to the hdfs plugin and this plugin will take care of sending the file to hdfs

)

(defn ^:dynamic load-channel []
  (let [topics (get-conf2 :kafka-hdfs-topics [])]
  {:start (fn [] 
              (channel-init)
              (prn "Using logs " topics)
              (.submit service (watch-critical-error process-topics topics)))
   
   :stop (fn []
           (.shutdown service)
           (.awaitTermination service 10000 TimeUnit/MILLISECONDS)
           (.shutdownNow service))
   }))

(let [{:keys [start stop]} (load-channel)]
       (register (->Channel ch-dsid start stop)))  
           

  



