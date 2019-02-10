NiFi Redis Publish / Subscribe processors
=====

The NiFi processor implements producer/consumer (queue) and publisher/subscriber (topic) patterns on top of Redis. Following options are available for now:

- `lpush`/`rpush`/`lpop`/`rpop` commands, so Redis lists are used to emulate a queue
- `publish`/`subscribe` commands, so pub/sub pattern are used to emulate a topic

Configuration options:
- `Redis Connection Pool` - standard NiFi controller service
- `Queue Mode` - xPUSH/xPOP or PUBLISH/SUBSCRIBE mode
- `Channel/Key Name` - channel (in case of pubsub mode) or key (for xPUSH/xPOP mode)

### How to build and run
Build NAR (NiFi Archive)
```
mvn install
```
Copy the NAR into your NiFi directory (usually `/opt/nifi/nifi-current/lib`). \
Restart NiFi.

To run all integration tests use
```
mvn -P integration-tests verify
```

### Release notes
##### v 0.0.2
Improved xPUSH operation performance by sending bulk of values per call. \
PUBLISH performance stays the same as far as Redis does not accepts nultiple values in the command.
##### v 0.0.1
First working release

### TODO
- Redis Streams support (now it is tricky to implement because Jedis still does not have streams support)
