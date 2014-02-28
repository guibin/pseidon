(defproject pseidon-kafka-hdfs "0.2.1-SNAPSHOT"
  :description "Pseidon pluging that copies data from kafka to hdfs"
  :url "https://github.com/gerritjvv/pseidon"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :warn-on-reflection true

  :plugins [
          [lein-rpm "0.0.5"] [lein-midje "3.0.1"] [lein-marginalia "0.7.1"]
          [lein-kibit "0.0.8"] [no-man-is-an-island/lein-eclipse "2.0.0"]
           ]

  :dependencies [
                 [fileape "0.4.0-SNAPSHOT"]
                 [com.taoensso/nippy "2.5.2"] 
                 [clj-json "0.5.3"]
                 [org.clojure/clojure "1.5.1"]]

  )
