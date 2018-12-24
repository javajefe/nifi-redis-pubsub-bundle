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

### TODO
- batch processing
- Redis Streams support
