
(defproject pseidon "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                 [org.clojure/clojure "1.5.1"]
                 [commons-net "3.2"]
                 [org.streams/streams-log "0.5.1"]
                 [org.clojure/tools.namespace "0.2.4-SNAPSHOT"]
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
		 [criterium "0.4.1"] 
                
                 ]
   :profiles {:dev {:dependencies [[midje "1.6-alpha2"]]
                    :plugins [[lein-midje "3.0.1"]]
                    }}

  :repositories {"sonatype-oss-public"
               "https://oss.sonatype.org/content/groups/public/"
               "streams-repo"
               "https://bigstreams.googlecode.com/svn/mvnrepo/releases"}
  
  
  :plugins [[lein-rpm "0.0.5"]]
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
