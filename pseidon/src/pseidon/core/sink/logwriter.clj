(ns pseidon.core.sink.logwriter)

(use '[pseidon.core.conf :as c])
(import '[scala.collection JavaConversions])
(import '[org.streams.streamslog.log.file FileLogResource])
(import '[org.streams.commons.compression.impl CompressionPoolFactoryImpl])
(import '[org.streams.streamslog.log.file TopicConfigParser])

(def codec (get c/conf "writer.codec" "org.apache.hadoop.io.compress.GzipCodec"))

;this module will use the bigstreams 
;class FileLogResource private (topics: Map[String, TopicConfig], compressors: Int = 100)
;org.streams.commons.compression.impl.CompressionPoolFactoryImpl
(def compressionPoolFactory (org.streams.commons.compression.impl.CompressionPoolFactoryImpl. 10 10 nil))

(defn get-topic-config [topic]

  (TopicConfigParser/apply "test:NOW:10000,1024:org.apache.hadoop.io.compress.GzipCodec:/tmp:true:10:true")
     
 )


(def res (FileLogResource/apply {"test" (get-topic-config "test")} 10) )

;sends the msg to the correct writer
(defn send-writer [topic msg]
     (.tell (.get res topic) msg)
 )



