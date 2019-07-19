# KBC Kafka Consumer

Simple Kafka consumer for Keboola Connection.


## Functionality

By default the app uses `SASL/SCRAM` for authentication.

The consumer persists its "commited" offsets in its own state so it is completely independent of commit states 
at Kafka and reading by other consumers in a same group won't affect it's setup. The last commited offset is used as 
a starting offset each consecutive run.

It is possible to override the starting offsets manually using `begin_offsets` parameter.

The extractor pulls only messages that haven't been downloaded yet until the last message that is present in 
each partition at the time of execution. Any messages produced at the time of extraction will be collected next run. 
If there is no new message, the extractor will finish without writing any results.


## Configuration parameters

- **servers** - [REQ] list of Kafka servers
- **group_id** - [REQ] Group ID of the consumer. Resulting in `[GROUP_ID]-consumer` ID.
- **topic** - [REQ] Topic ID
- **username** - [REQ] Username (For SASL/SCRAM auth by default)
- **#password** - [REQ] Password
- **begin_offsets** - [OPT] Optional argument allowing specification of starting offset for each partition.
It is an object with attribute key marking the partition number prefixed by `p` and offset number. 
e.g. `{"p2": 0, "p1": 1, "p4": 0, "p0": 1, "p3": 3}`
- **debug** - [OPT] Optional argument to enable debug mode with extensive logging. By default `false`

