(ns pseidon.core.datastore
  (:use clojure.tools.logging
        pseidon.core.conf
        
        ))

(def client (ref nil))

(defn get-client ^org.apache.curator.framework.CuratorFramework []; CuratorFramework    client = CuratorFrameworkFactory.builder().namespace("MyApp") ... build();
  (if (nil? @client)
      (dosync (alter client 
                     (fn [p]
                       (let [retry-policy (org.apache.curator.retry.ExponentialBackoffRetry. 1000 10)
                           bclient (-> (org.apache.curator.framework.CuratorFrameworkFactory/builder) (.namespace (get-conf2 :zk-ns "pseidon") )  (.connectString (get-conf :zk-url) ) (.retryPolicy retry-policy)  .build   )
                           ]
                           (.start bclient)
                           bclient
                        ))
       ))
       @client
      )
  )

(defn shutdown[]
 "
   Application level method and calls close on the client.
 "  
  (dosync 
        (alter client 
               (fn [p]
                   (if (not (nil? p)) (.close p))
                   nil ;set the client to nil
                 ))))


(defn get-bytes [value]
  (pseidon.util.Bytes/toBytes value))
  
(defn ensure-path [client ns path]
  (let [p (clojure.string/join "/" [ns path])]
  (if (not (-> client .checkExists (.forPath p)))
     (do 
       (def create (fn  [dirs dir] 
                       (let [p2 (clojure.string/join "/" [dirs dir])]
                                   (if (not (-> client .checkExists (.forPath p2))) (-> client .create (.withMode (org.apache.zookeeper.CreateMode/PERSISTENT)) (.withACL org.apache.zookeeper.ZooDefs$Ids/OPEN_ACL_UNSAFE) 
                                                                                      (.forPath p2) 
                                      ))
                                     (clojure.string/join "/" [dirs dir])
                     )))
       (reduce create (clojure.string/split p #"/"))
     ))
     p ;return the path
  ))

(defn set-data! [ns id value]
  "Set a data structure's value the value must be a String or Number type"
   (let [
         p (ensure-path (get-client) ns id )
         f #(-> %1 .setData (.forPath p (get-bytes value)) ) ]
    (f (get-client))
    value
  ))


(defn mkdirs [ns & dirs]
  "
    Takes directories as a b c .. and joins then by '/'
    The first argument must always begin with '/'
    Create all the directories if the do not exist
  "
  (let [p (clojure.string/join "/" dirs)
        bclient (get-client)]
        (ensure-path bclient ns p)
        ))

(defn list-dirs [ & dirs]
  "Takes directories as a b c .. and joins then by '/'
   The children of that directory is called, note that the first argument must always begin with '/'
   incase of an exception an empty vector is returned
   Directories are always returned in lexical sorted order
  "
  (try (let [dir (clojure.string/join "/" dirs)
        f #(-> %1 .getChildren (.forPath dir) sort)]
   (f (get-client))  
  )
  (catch Exception e [] )
  ))

(defn get-data [ns id]
  "Gets the value of a data in bytes"
  (let [f #(-> %1 .getData (.forPath (ensure-path (get-client) ns id )))  ]
     (f (get-client))
  ))


(defn get-data-str [ns id]
  "Gets the value of a data as a string encoded in utf-8"
  (pseidon.util.Bytes/toString (get-data ns id))
  )

(defn get-data-int [ns id]
  "Gets the value of a data as a 4 byte integer"
   (pseidon.util.Bytes/toInt (get-data ns id)))


(defn get-data-long [ns id]
  "Gets the value of a data as a 8 byte long value"
  (pseidon.util.Bytes/toLong (get-data ns id))
  )


(defn inc-bytes[bts val]
   (println "Class of val " (class bts))
   (if (= (.length bts) 4)
       (+ (pseidon.util.Bytes/toInt bts) val)
       (if (= (.length bts) 8)
           (+ (pseidon.util.Bytes/toLong bts) val)
           (throw Exception "Type with byte length " (.length bts) " cannot be incremented" ))
     )  
  )

(defn inc-data! [ns id inc-val]
  "Increments the data value by the inc-val, the value must be numeric long or int"
    (set-data! ns id (inc-bytes (get-data ns id) inc-val) ))


(defn dec-data! [ns id dec-val]
  "Decrements the data value by the dec-val the value must be numeric long or int"
   (inc-data! (* -1 dec-val)))

