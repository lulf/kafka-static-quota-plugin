# Kafka Static Quota Plugin

This is a broker quota plugin for Apache Kafka to allow setting a per-broker limits statically in
configuration. The quota plugin assigns a shared quota across all client ids and users.

To build the plugin:

```
./gradlew shadowJar
```

Copy the resulting jar in `build/libs/kafka-static-quota-plugin-all.jar` into the Kafka classpath.

Configure Kafka to load the plugin and some plugin properties:

```
client.quota.callback.class=org.apache.kafka.server.quota.StaticQuotaCallback
broker.quota=1000000
```

The quota is given in bytes, and will translate to bytes/sec for your producer for instance.

## Testing locally

Run it locally (make sure your server.properties enables the reporter):

```
CLASSPATH=/path/to/build/libs/kafka-static-quota-plugin-all.jar ./bin/kafka-server-start.sh server.properties
```
