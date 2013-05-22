;(ns plugins.sinks.hdfs)
;(use '[pseidon.core.conf :as c])

;(import '[scala.collection JavaConversions])
;(import '[org.streams.streamslog.log.file FileLogResource])
;(import ['org.streams.commons.compression.impl CompressionPoolFactoryImpl])
;(import '[org.streams.streamslog.log.file TopicConfigParser])

;(def codec (get c/conf "writer.codec" "org.apache.hadoop.io.compress.GzipCodec")

;this module will use the bigstreams 
;class FileLogResource private (topics: Map[String, TopicConfig], compressors: Int = 100)
;org.streams.commons.compression.impl.CompressionPoolFactoryImpl
;(def compressionPoolFactory (org.streams.commons.compression.impl.CompressionPoolFactoryImpl. 10 10 nil))

;(defn get-topic-config [topic]

  ;(TopicConfigParser/apply "test:NOW:10000,1024:org.apache.hadoop.io.compress.GzipCodec:/tmp:true:10:true")
     
 ; )

;(;def res (FileLogResource/apply {"test" (get-topic-config "test")} 10) )

;(defn get-writer [topic]
  
 ; )
;
;TopicConfig = TopicConfigParser /**
  ;* Parse a line of format topic:NOW/script_file:timeout,sizeInBytes,compressionCodecClass:basedir:base64,threads,useNewLine
  ;*/
  ;def apply(line: String) = {
                             
                             
;

;CompressionPoolFactory




;
;org.streams.streamslog.log.file
;object LogFileWriter {

 ; def apply(topicConfig: TopicConfig, compressionPoolFactory: CompressionPoolFactory, statusActor: ActorRef = null) = {
  ;  FileLogResource.system.actorOf(Props(new LogFileWriter(topicConfig, compressionPoolFactory, statusActor)), "logWriter-" + topicConfig.topic)
  ;}

;}