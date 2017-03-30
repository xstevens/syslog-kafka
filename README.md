# DISCLAIMER
YOU PROBABLY DO NOT WANT TO USE THIS.

This was an open-source thought experiment I had a few years ago, when there wasn't a lot of support for Kafka within the existing syslog daemons. I had written a closed source implementation on top of Netty at the time for my employer. These days the major syslog daemons support publishing to Kafka, so I would recommend using one of the following:

* rsyslog [omfkafka](http://www.rsyslog.com/doc/master/configuration/modules/omkafka.html)
* syslog-ng [kafka](https://www.balabit.com/documents/syslog-ng-ose-latest-guides/en/syslog-ng-ose-guide-admin/html/configuring-destinations-kafka.html)

# syslog-kafka #

Version: 0.1-SNAPSHOT

#### A syslog daemon for producing messages to Apache Kafka. ####


### Version Compatability ###
This code is built with the following assumptions.  You may get mixed results if you deviate from these versions.

* [Syslog4j](http://www.syslog4j.org/) 0.9.30
* [Kafka](http://incubator.apache.org/kafka) 0.7.2
* [Protocol Buffers](https://developers.google.com/protocol-buffers) 2.4.1

### Prerequisites ###
* Protocol Buffers
* Zookeeper (for Kafka)
* Kafka

### Setup ###
You will need to setup Google's Protocol Buffers in order for this project to compile and run. Since this is fairly well documented and straightforward I'll leave that up to the user. Most platforms that have a descent packaging system will probably have a protobuf package already. If you're on a Mac using Homebrew this is as easy as:

`brew install protobuf`

You will also need Apache Kafka. As of this writing the latest is Kafka 0.7.2 but the project is still in the incubation stage. Unfortunately this means they can't push an official Apache Kafka jar to Maven repositories. So you'll need to add it to your environment manually. This can be done in the following steps:

    curl https://www.apache.org/dyn/closer.cgi/incubator/kafka/kafka-0.7.2-incubating/kafka-0.7.2-incubating-src.tgz -o kafka-0.7.2-incubating-src.tgz
    tar xvzf kafka-0.7.2-incubating-src.tgz
    cd kafka-0.7.2-incubating-src
    ./sbt update
    ./sbt package
    cd ./core/target/scala_2.8.0/
    mvn install:install-file -Dfile=kafka-0.7.2.jar -DgroupId=org.apache.kafka -DartifactId=kafka-core -Dversion=0.7.2-incubating -Dpackaging=jar
    
If everything went okay you should now have a kafka-0.7.2.jar in your local Maven repository.

### Building ###
To make a jar you can do:  

`mvn package`

The jar file is then located under `target`.

### Running an instance ###
**Make sure your Kafka and Zookeeper servers are running first (see Kafka documentation)**

In order to run syslog-kafka on another machine you will probably want to use the _dist_ assembly like so:

`mvn assembly:assembly`

The zip file now under the `target` directory should be deployed to `SYSLOGKAFKA_HOME` on the remote server.

** MORE DOCUMENTATION TO FOLLOW. THIS IS A WORK IN PROGRESS IN MY SPARE TIME. **

### Example Kafka Producer Configuration (conf/kafka.producer.properties) ###
    # comma delimited list of ZK servers
    zk.connect=127.0.0.1:2181
    # use syslog message encoder
    serializer.class=kafka.serializer.SyslogMessageEncoder
    # asynchronous producer
    producer.type=async
    # compression.code (0=uncompressed,1=gzip,2=snappy)
    compression.codec=2
    # batch size (one of many knobs to turn in kafka depending on expected data size and request rate)
    batch.size=100

### License ###
All aspects of this software are distributed under Apache Software License 2.0. See LICENSE file for full license text.

### Contributors ###

* Xavier Stevens ([@xstevens](http://twitter.com/xstevens))
