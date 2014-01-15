(defproject pseidon-kafka "0.4.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [
                 [org.clojure/core.async "0.1.267.0-0d7780-alpha"]
                 [kafka-clj "0.2.0-SNAPSHOT"]
                 [com.yammer.metrics/metrics-core "2.2.0"]
                 [com.yammer.metrics/metrics-annotation "2.2.0"]
                 [com.yammer.metrics/metrics-core "2.2.0" :scope "test"]
                 [com.github.sgroschupf/zkclient "0.1" :scope "test"]
                 [midje "1.6-alpha2" :scope "test"]
                 [pseidon "0.4.2-SNAPSHOT" :scope "provided"]
                 [com.taoensso/nippy "2.5.2"]
                 [night-vision "0.1.0-SNAPSHOT" :scope "test"]
                 [org.apache.zookeeper/zookeeper "3.4.5"]
                 [net.sf.jopt-simple/jopt-simple "3.2"]
                 [clj-tuple "0.1.4"]
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
   :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]
   :java-source-paths ["java"]
  
  )
