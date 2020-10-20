# Kafka Static Quota Plugin

This is a broker quota plugin for Apache Kafka to allow setting a per-broker limits statically in
the broker configuration. 

The default quota plugin in Apache Kafka will hand out a unique quota per client. This plugin will configure a total
quota independent of the number of clients. For example, if you have configured a produce
quota of 40 MB/second, and you have 10 producers running as fast as possible, they will be limited by 4 MB/second each. 

The quota distribution across clients is not static. If you have a max of 40 MB/second, 2 producers, and one of them is 
producing messages at 10 MB/second, the second producer will be throttled at 30 MB/second.

## Building

To build the plugin:

```
./gradlew shadowJar
```

Copy the resulting jar in `build/libs/kafka-static-quota-plugin-all.jar` into the Kafka classpath.


## Configuring

Configure Kafka to load the plugin and some plugin properties:

```
client.quota.callback.class=org.apache.kafka.server.quota.StaticQuotaCallback
client.quota.callback.static.produce=1000000
client.quota.callback.static.fetch=1000000
```

The quota is given in bytes, and will translate to bytes/sec in total for your clients.

## Testing locally

Run it locally (make sure your server.properties enables the reporter):

```
CLASSPATH=/path/to/build/libs/kafka-static-quota-plugin-all.jar ./bin/kafka-server-start.sh server.properties
```
