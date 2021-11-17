# KBC Kafka Consumer

Simple Kafka consumer for Keboola Connection.


## Functionality

By default the app uses `SASL/SCRAM` for authentication. ("security.protocol": "SASL_SSL")

The consumer persists its "commited" offsets in its own state so it is completely independent of commit states 
at Kafka and reading by other consumers in a same group won't affect it's setup. The last commited offset is used as 
a starting offset each consecutive run.

It is possible to override the starting offsets manually using `begin_offsets` parameter.

The extractor pulls only messages that haven't been downloaded yet until the last message that is present in 
each partition at the time of execution. Any messages produced at the time of extraction will be collected next run. 
If there is no new message, the extractor will finish without writing any results.


## Configuration parameters

- **servers** - [REQ] list of Kafka servers. Bootstrap Servers are a list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
 These servers are just used for the initial connection to discover the full cluster membership.
- **group_id** - [REQ] Group ID of the consumer. Resulting in `[GROUP_ID]-consumer` ID. The consumer group is used for coordination between consumer. 
 Since the app contains a single consumer and maintains the offset itself, it can be an arbitrary value.
- **topic** - [REQ] Topic ID
- **username** - [REQ] Username (For SASL/SCRAM auth by default)
- **#password** - [REQ] Password
- **begin_offsets** - [OPT] Optional argument allowing specification of starting offset for each partition.
It is an object with attribute key marking the partition number prefixed by `p` and offset number. 
e.g. `{"p2": 0, "p1": 1, "p4": 0, "p0": 1, "p3": 3}`
- **debug** - [OPT] Optional argument to enable debug mode with extensive logging. By default `false`

