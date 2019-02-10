package org.javajefe.nifi.processors.redis.pubsub;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.redis.service.RedisConnectionPoolService;
import org.apache.nifi.redis.util.RedisUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * This is an integration test that is meant to be run against a real Redis instance.
 */
public abstract class ITAbstractRedis {

    protected static final String KEY = "redis.pubsub.it-test.key";
    protected TestRunner testRunner;
    protected Jedis jedis;
    protected List<String> messages;
    private RedisServer redisServer;
    private RedisConnectionPoolService redisConnectionPool;

    protected void setup(Processor proc) throws IOException, InitializationException {
        String redisHost = "localhost";
        int redisPort = getAvailablePort();
        this.redisServer = new RedisServer(redisPort);
        redisServer.start();

        testRunner = TestRunners.newTestRunner(proc);
        // create, configure, and enable the RedisConnectionPool service
        redisConnectionPool = new RedisConnectionPoolService();
        testRunner.addControllerService("redis-connection-pool", redisConnectionPool);
        testRunner.setProperty(redisConnectionPool, RedisUtils.CONNECTION_STRING, redisHost + ":" + redisPort);
        testRunner.enableControllerService(redisConnectionPool);

        this.jedis = new Jedis(redisHost, redisPort);
        jedis.del(KEY);
    }

    private int getAvailablePort() throws IOException {
//        try (SocketChannel socket = SocketChannel.open()) {
//            socket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
//            socket.bind(new InetSocketAddress("localhost", 0));
//            return socket.socket().getLocalPort();
//        }
        return 6379;    // TODO Wait for NIFI-5830 (1.9.0)
    }

    protected void teardown() throws IOException {
        if (jedis != null && jedis.isConnected()) {
            jedis.close();
        }
        if (redisConnectionPool != null) {
            redisConnectionPool.onDisabled();
        }
        if (redisServer != null) {
            redisServer.stop();
        }
        testRunner.shutdown();
    }

    protected void generateMessages(int number) {
        messages = IntStream.range(0, number).mapToObj(i -> "trigger-" + i).collect(toList());
    }
}
