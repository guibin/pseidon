(defproject pseidon-kafka "0.2.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :resource-paths ["deps/kafka/kafka-0.7.2-2.9.2.jar"]

  :dependencies [

                 [org.apache.kafka/kafka-core "0.7.2"]
                 [com.github.sgroschupf/zkclient "0.1"]
                 [org.scala-lang/scala-library "2.8.0"]
                 
                 [com.yammer.metrics/metrics-core "2.2.0" :scope "test"]
                 [com.github.sgroschupf/zkclient "0.1" :scope "test"]
                 [midje "1.6-alpha2" :scope "test"]
                 [pseidon "0.3.1-SNAPSHOT" :scope "provided"]
                 [night-vision "0.1.0-SNAPSHOT" :scope "test"]
                 [org.apache.zookeeper/zookeeper "3.4.5"]
                 
                 [org.clojure/clojure "1.5.1" :scope "provided"]
                 [reply "0.1.0-beta9" :scope "provided"]
                 [jline "2.11" :scope "provided"] ;need for dependency from reply
                 ]

  :warn-on-reflection true

  :plugins [
          [lein-rpm "0.0.5"] [lein-midje "3.0.1"] [lein-marginalia "0.7.1"]
          [lein-kibit "0.0.8"] [no-man-is-an-island/lein-eclipse "2.0.0"]
           ]

   :aot [pseidon.kafka.core]
   :main pseidon.kafka.core
  
  )
