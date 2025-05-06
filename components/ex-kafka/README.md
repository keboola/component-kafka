# KBC Kafka Consumer

Simple Kafka consumer for Keboola Connection.


## Functionality

Component supports following security protocols: PLAINTEXT, SASL_PLAINTEXT, SSL

Message payload can be either stored raw in the single column of output table or deserialized by JSON or Avro deserializer.

The schema for Avro deserialization can be provided as schema string, or obtained from the schema registry if configured.
The component was tested with the Confluent Schema Registry
If the payload is deserialized, it can be stored either as json in column, or all values flattened to columns.

The consumer persists its "committed" offsets in its own state so it is completely independent of commit states 
at Kafka and reading by other consumers in a same group won't affect its setup. The last committed offset is used as 
a starting offset each consecutive run.

It is possible to override the starting offsets manually using `begin_offsets` parameter.

The extractor pulls only messages that haven't been downloaded yet until the last message that is present in 
each partition at the time of execution. Any messages produced at the time of extraction will be collected next run. 
If there is no new message, the extractor will finish without writing any results.


## Configuration parameters - Application

- **servers** - [REQ] list of Kafka servers. Bootstrap Servers are a list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
 These servers are just used for the initial connection to discover the full cluster membership.
- **group_id** - [REQ] Group ID of the consumer. Resulting in `[GROUP_ID]-consumer` ID. The consumer group is used for coordination between consumer. 
 Since the app contains a single consumer and maintains the offset itself, it can be an arbitrary value.
- **security_protocol** - [REQ] Security protocol. Possible values: `PLAINTEXT`, `SASL_PLAINTEXT`, `SSL`
- **username** - [REQ] Username required if `security_protocol` is set to `SASL_PLAINTEXT`
- **#password** - [REQ] Password required if `security_protocol` is set to `SASL_PLAINTEXT`
- **#ssl_ca** - [REQ] CA certificate as string. Required if `security_protocol` is set to `SSL`
- **#ssl_key** - [REQ] Client key as string. Required if `security_protocol` is set to `SSL`
- **#ssl_certificate** - [REQ] Client certificate as string. Required if `security_protocol` is set to `SSL`
- **begin_offsets** - [OPT] Optional argument allowing specification of starting offset for each partition.
It is an object with attribute key marking the partition number prefixed by `p` and offset number. 
e.g. `{"p2": 0, "p1": 1, "p4": 0, "p0": 1, "p3": 3}`
- **debug** - [OPT] Optional argument to enable debug mode with extensive logging. By default `false`
- **kafka_extra_params** - [OPT] Optional argument to specify extra parameters for Kafka consumer. By default `""`

## Configuration parameters - row

- **topic** - [REQ] list of Kafka topics to consume, can be obtained by "Load topics" button
- **deserialize** - [REQ] Deserialization method. Possible values: `no`, `avro`
- **flatten_message_value_columns** - [OPT] Optional argument to enable flattening of Avro deserialized message value columns. By default `false`
- **schema_source** - [OPT] Optional argument to specify source of Avro schema. Possible values: `user_defined`, `schema_registry`. By default `string`
- **schema_str** - [OPT] Optional argument to specify Avro schema as string. Required if `schema_source` is set to `user_defined`
- **schema_registry_url** - [OPT] Optional argument to specify URL of Schema Registry. Required if `schema_source` is set to `schema_registry`
- **schema_registry_extra_params** - [OPT] Optional argument to specify extra parameters for Schema Registry. By default `""`


### Example Application configuration JSON

```
{
  "parameters": {
    "kafka_extra_params": "{\"session.timeout.ms\": 6000 }",
    "servers": [
      "xxx01.srvs.test.com:9094",
      "xxxy-02.srvs.test.com:9094",
      "xxx-03.srvs.test.com:9094"
    ],
    "sasl_mechanisms": "PLAIN",
    "username": "user",
    "security_protocol": "SASL_PLAINTEXT",
    "#password": "KBC::ProjectSecure::...",
    "debug": true
  }
}
```

### Example row configuration JSON

```
{
  "parameters": {
    "topics": [
      "first",
      "second"
    ],
    "deserialize": "avro",
    "flatten_message_value_columns": true,
    "schema_source": "schema_registry",
    "schema_registry_url": "http://schema-registry:8081",
    "schema_registry_extra_params": ""
  }
}
```