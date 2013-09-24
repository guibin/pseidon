# pseidon-kafka

Provides a DataSource and Sink pluging to psiedon for kafka.

## Handling failures and avoid message loss

Rather than implementing the low level kafka api and trying to keep track of every message in zookeeper another route is taken.
Each group of messages retreived are given an id value that consists of the topic,parition,min-offset,max-offset. 
Its up to the user of this datasource to mark the message group id as done. 
When failure is detected the datasource on startup will scan for any message groups with status "ready/run" and replay them from kafka.

This allows the datasource to use the highlevel api for kafka, and not complicate the logic with what is essentially kafka internals.


The topic, partition and offset can be retreived from the MessageAndMetadata instance.

## Usage


## License

Copyright © 2013 FIXME

Distributed under the Eclipse Public License, the same as Clojure.
