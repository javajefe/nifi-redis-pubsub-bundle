package org.javajefe.nifi.processors.redis.pubsub;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.redis.service.RedisConnectionPoolService;
import org.apache.nifi.redis.util.RedisUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.*;
import static org.junit.Assert.*;

/**
 * This is an integration test that is meant to be run against a real Redis instance.
 */
public class ITPublishRedis {

    private static final String KEY = "key";
    private PublishRedis proc;
    private TestRunner testRunner;
    private RedisServer redisServer;
    private RedisConnectionPoolService redisConnectionPool;
    private Jedis jedis;
    private int redisPort;

    @Before
    public void setup() throws IOException, InitializationException {
        this.redisPort = getAvailablePort();

        this.redisServer = new RedisServer(redisPort);
        redisServer.start();
        this.jedis = new Jedis("localhost", redisPort);

        proc = new PublishRedis();
        testRunner = TestRunners.newTestRunner(proc);

        // create, configure, and enable the RedisConnectionPool service
        redisConnectionPool = new RedisConnectionPoolService();
        testRunner.addControllerService("redis-connection-pool", redisConnectionPool);
        testRunner.setProperty(redisConnectionPool, RedisUtils.CONNECTION_STRING, "localhost:" + redisPort);

        testRunner.enableControllerService(redisConnectionPool);
    }

    private int getAvailablePort() throws IOException {
//        try (SocketChannel socket = SocketChannel.open()) {
//            socket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
//            socket.bind(new InetSocketAddress("localhost", 0));
//            return socket.socket().getLocalPort();
//        }
        return 6379;    // TODO Wait for NIFI-5830 (1.9.0)
    }

    @After
    public void teardown() throws IOException {
        if (redisConnectionPool != null) {
            redisConnectionPool.onDisabled();
        }
        if (jedis != null && jedis.isConnected()) {
            jedis.close();
        }
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    @Test
    public void testLPUSH() {
        int numberOfFlowFiles = 10;
        List<String> flowFilesPayload = IntStream.range(0, numberOfFlowFiles).mapToObj(i -> "trigger-" + i).collect(toList());
        publish(proc.LPUSH_MODE, flowFilesPayload);

        assertEquals("list", jedis.type(KEY));
        assertEquals(numberOfFlowFiles, jedis.llen(KEY).longValue());
        List<String> value = jedis.lrange(KEY, 0, -1);
        Collections.reverse(value);
        assertArrayEquals(flowFilesPayload.toArray(), value.toArray());
    }

    @Test
    public void testRPUSH() {
        int numberOfFlowFiles = 10;
        List<String> flowFilesPayload = IntStream.range(0, numberOfFlowFiles).mapToObj(i -> "trigger-" + i).collect(toList());
        publish(proc.RPUSH_MODE, flowFilesPayload);

        assertEquals("list", jedis.type(KEY));
        assertEquals(numberOfFlowFiles, jedis.llen(KEY).longValue());
        List<String> value = jedis.lrange(KEY, 0, -1);
        assertArrayEquals(flowFilesPayload.toArray(), value.toArray());
    }

    @Test
    public void testPUBLISH() throws InterruptedException {
        final Deque<String> channelsDeque = new ConcurrentLinkedDeque<>();
        final Deque<String> messagesDeque = new ConcurrentLinkedDeque<>();
        ExecutorService threadPool = Executors.newSingleThreadExecutor();
        threadPool.submit(() -> jedis.subscribe(new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                channelsDeque.addLast(channel);
                messagesDeque.addLast(message);
            }
        }, KEY));

        int numberOfFlowFiles = 10;
        List<String> flowFilesPayload = IntStream.range(0, numberOfFlowFiles).mapToObj(i -> "trigger-" + i).collect(toList());
        publish(proc.PUBLISH_MODE, flowFilesPayload);

        Thread.sleep(2000);
        threadPool.shutdown();
        threadPool.awaitTermination(2, TimeUnit.SECONDS);
        assertArrayEquals(flowFilesPayload.toArray(), messagesDeque.toArray());
        assertEquals("Invalid channel", numberOfFlowFiles, channelsDeque.size());
        assertTrue("Invalid channel", channelsDeque.stream().allMatch(channel -> channel.equals(KEY)));
    }

    private void publish(AllowableValue queueMode, List<String> flowFilesPayload) {
        testRunner.setProperty(proc.REDIS_CONNECTION_POOL, "redis-connection-pool");
        testRunner.setProperty(proc.QUEUE_MODE, queueMode);
        testRunner.setProperty(proc.CHANNEL_OR_LIST, KEY);

        // queue a flow file to trigger the processor and executeProcessor it
        flowFilesPayload.forEach(payload -> testRunner.enqueue(payload));
        testRunner.run(flowFilesPayload.size());
        testRunner.assertAllFlowFilesTransferred(PublishRedis.REL_SUCCESS, flowFilesPayload.size());
    }
}
