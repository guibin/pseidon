(defproject pseidon-hdfs "0.1.0-SNAPSHOT"
  :description "A pseidon data source, channel and processor for writing to hdfs"
  :url "github.com/gerritjvv/pseidon"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
		
    [org.clojure/clojure "1.5.1" :scope "provided"]
    [org.apache.hadoop/hadoop-hdfs "2.0.0-cdh4.2.0" :scope "provided"]
    [org.apache.hadoop/hadoop-minicluster "2.0.0-mr1-cdh4.2.0" :scope "test"]
    [pseidon "0.3.0-SNAPSHOT" :scope "provided"]
    [commons-lang "2.6" :scope "provided"]
    [org.clojure/core.async "0.1.0-SNAPSHOT" :scope "provided"]
    [midje "1.6-alpha2" :scope "test"]
     
		]
  :repositories {
		 "cloudera"
 		 "https://repository.cloudera.com/artifactory/cloudera-repos/"
	         "sonatype-oss-public" 
		 "https://oss.sonatype.org/content/groups/public/"
          	}
  
  
  :plugins [
            [lein-midje "3.0.1"] 
            [lein-marginalia "0.7.1"] 
            [lein-kibit "0.0.8"]
           ]
  :warn-on-reflection true
  
		)


