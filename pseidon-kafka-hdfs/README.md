# pseidon-kafka-hdfs

Reads from kafka and sends the data in rolled files to hdfs

## Usage

### Encoders and Decoders:

Data read from kafka can be decoded and encoded into different formats, e.g. if the data in kafka is in nippy, 
and you want to save it as json to hadoop, you can use the :nippy decoder and the json encoder.

The default encoder and decoder are identity functions and does nothing, encoders and decoders can be specified
on a topic per topic basis and or globally.

The configuration properties are:
```
kafka-hdfs-$topic-encoder
kafka-hdfs-$topic-decoder
kafka-hdfs-default-encoder
kafka-hdfs-default-decoder
```

And supported values are:

``` :nippy :json :default ```


### Time parsers

The time stamp can be taken from the kafka message, or the current time when the message is read can be used.

The property to specify this is ```kafka-hdfs-ts-parser or kafka-hdfs-$topic-ts-parser``` possible values are ```:now``` this takes the current time stamp,
```:obj``` this takes the path from a object map message structure, note to use this the decoder must be ```:json``` or ```:nippy```.

#### Timestamp path

The library doesn't make assumption of where the timestamp is, and so its required to give a timestamp path, 
this can be done on a topic basis or globally for all messages.

```
:kafka-hdfs-$topic-ts-parser-args
:kafka-hdfs-ts-parser-args
```

The value must be a vector or list.

The default used s ```clojure ["ts"]```

## License

Copyright © 2013 FIXME

Distributed under the Eclipse Public License, the same as Clojure.
