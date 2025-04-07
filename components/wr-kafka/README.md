# KBC Kafka Writer

Simple Kafka producer for Keboola Connection.

## Functionality

Component supports following security protocols: PLAINTEXT, SASL_PLAINTEXT, SSL

Component supports following serializations of value part of the message: text / JSON / Avro

Messages can be serialized using Avro serialization if configured. The schema for serialization can be provided as a
schema string, or obtained from the schema registry if configured.

The component reads data from input tables and produces messages to a Kafka topic. The message key can be set from a
specified column, and the message value can include selected columns from the input data.

## Configuration parameters - Application

- **servers** - [REQ] list of Kafka servers. Bootstrap Servers are a list of host/port pairs to use for establishing the
  initial connection to the Kafka cluster.
  These servers are just used for the initial connection to discover the full cluster membership.
- **client_id** - [OPT] Client ID of the producer.
- **topic** - [REQ] Kafka topic to produce messages to.
- **security_protocol** - [REQ] Security protocol. Possible values: `PLAINTEXT`, `SASL_PLAINTEXT`, `SSL`
- **sasl_mechanisms** - [OPT] SASL mechanism to use for authentication. Required if `security_protocol` is set to
  `SASL_PLAINTEXT`
- **username** - [REQ] Username required if `security_protocol` is set to `SASL_PLAINTEXT`
- **#password** - [REQ] Password required if `security_protocol` is set to `SASL_PLAINTEXT`
- **#ssl_ca** - [REQ] CA certificate as string. Required if `security_protocol` is set to `SSL`
- **#ssl_key** - [REQ] Client key as string. Required if `security_protocol` is set to `SSL`
- **#ssl_certificate** - [REQ] Client certificate as string. Required if `security_protocol` is set to `SSL`
- **kafka_extra_params** - [OPT] Optional argument to specify extra parameters for Kafka producer as JSON string.
- **key_column_name** - [OPT] Name of the column in input table to use as message key. Default is empty string.
- **value_column_names** - [OPT] List of column names in input table to include in the message value. Default is an
  empty list.
- **serialize** - [OPT] Serialization method. Possible values: `text`, `avro`, `json`,
- **schema_str** - [OPT] Argument to specify Avro schema as string. Required if `serialize` is set to `avro`.
- **schema_registry_url** - [OPT] Optional argument to specify URL of Schema Registry. Required if using schema registry
  for Avro serialization.
- **schema_registry_extra_params** - [OPT] Optional argument to specify extra parameters for Schema Registry as JSON
  string.
- **debug** - [OPT] Optional argument to enable debug mode with extensive logging. By default `false`

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
    "key_column_name": "order_id"
    "value_column_names": [
      "name",
      "address"
    ],
    "serialize": "json",
  }
}
```