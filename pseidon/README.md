# pseidon

A Clojure library designed to ... well, that part is up to you.

## Usage

Big Data Imports


## Downloads

https://sourceforge.net/projects/pseidon/files

## Doc

### Classpath setup and including new plugins

The classpath for pseidon is set using the classpath property in the pseidon.edn file

The default property value is /opt/pseidon/lib

To add in a new plugin with its own jars and files edit this property.

### Metrics

Pseidon uses the http://metrics.codahale.com/ library to show metrics.

Plugins can use the pseidon.core.metrics namespace to add metrics.

To see metrics for a running instance 

Open in a browser:

[http://localhost:8281/metrics](http://localhost:8282/metrics)

### Tracking

Each message send from a datasource is tracked through the system with two states
['running', 'done'].

'running' is when the message has leaved the datasource

'done' is when the message has be fully processed and can be dicarded

Open in a browser:

[http://localhost:8282/tracking?max=10&from=0](http://localhost:8282/tracking?max=10&from=0)


or to apply a query clause

[http://localhost:8282/tracking?q="dsid='myid' and status='running'"&max=10&from=0](http://localhost:8282/tracking?q="dsid='myid' and status='running'"&max=10&from=0)

There are three columns you can query:

  * dsid String
  * status String
  * ts TimeStamp format

### Registry

Open in a browser:

[http://localhost:8282/registry](http://localhost:8282/registry)


## License

Apache License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0.html



