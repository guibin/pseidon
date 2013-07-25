
(defproject pseidon "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                 [org.clojure/clojure "1.5.1"]
                 [commons-net "3.2"]
                 [commons-lang "2.6"]
                 [commons-io "2.4"]
                 [org.apache.commons/commons-vfs2 "2.0"]
                 [org.apache.sshd/sshd-core "0.8.0"]  
                 [com.jcraft/jsch "0.1.50"]
                 [org.streams/streams-log "0.5.1"]
                 [org.clojure/tools.namespace "0.2.4-SNAPSHOT"]
                 [clj-time "0.5.1"]
                 ;[prismatic/plumbing "0.1.0"]
                 ;[org.clojure/core.logic "0.8.3"]
                 [org.clojure/tools.cli "0.2.2"]
                 
                 [log4j/log4j "1.2.16" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]
                 [clj-logging-config "1.9.10"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/tools.nrepl "0.2.3"]
                 [org.apache.curator/curator-framework "2.0.1-incubating"]
   		           [org.apache.curator/curator-test "2.0.1-incubating" :scope "test"]
                 [midje "1.6-alpha2" :scope "test"]
                 [cupboard "1.0beta1"]
                 [criterium "0.4.1"] ;benchmarking
                 [spyscope "0.1.3" :scope "test"]
                
                 ]
  :aot [pseidon.core]
  :main pseidon.core
  :repositories {"sonatype-oss-public"
               "https://oss.sonatype.org/content/groups/public/"
               "streams-repo"
               "https://bigstreams.googlecode.com/svn/mvnrepo/releases"}
  
  :java-source-paths ["java"]
  
  :plugins [
         [lein-rpm "0.0.5"] [lein-midje "3.0.1"] [lein-marginalia "0.7.1"] 
         [lein-kibit "0.0.8"]
           ]

  :rpm {:name "pseidon"
        :summary "pseidon streaming imports"
        :copyright "Apache-2 Licence"
        :workarea "target"
        :mappings [{:directory "/opt/pseidon/lib"
                    :filemode "440"
                    :username "root"
                    :groupname "root"
                    ;; There are also postinstall, preremove and postremove
                    :sources {:source [{:location "target/classes"}
                                       {:location "src"}]
                           }}]}
  )
