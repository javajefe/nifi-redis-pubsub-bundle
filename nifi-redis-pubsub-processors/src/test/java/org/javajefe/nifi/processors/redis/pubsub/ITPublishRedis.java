package org.javajefe.nifi.processors.redis.pubsub;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.reporting.InitializationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.JedisPubSub;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * This is an integration test that is meant to be run against a real Redis instance.
 */
public class ITPublishRedis extends ITAbstractRedis {

    @Before
    public void setup() throws IOException, InitializationException {
        setup(new PublishRedis());
    }

    @After
    public void teardown() throws IOException {
        super.teardown();
    }

    @Test
    public void testLPUSH() {
        generateMessages(10);
        init(PublishRedis.LPUSH_MODE);
        publish();

        assertEquals("list", jedis.type(KEY));
        List<String> value = jedis.lrange(KEY, 0, -1);
        Collections.reverse(value);
        assertArrayEquals(messages.toArray(), value.toArray());
    }

    @Test
    public void testRPUSH() {
        generateMessages(10);
        init(PublishRedis.RPUSH_MODE);
        publish();

        assertEquals("list", jedis.type(KEY));
        List<String> value = jedis.lrange(KEY, 0, -1);
        assertArrayEquals(messages.toArray(), value.toArray());
    }

    @Test
    public void testPUBLISH() throws InterruptedException {
        generateMessages(10);
        init(PublishRedis.PUBLISH_MODE);
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

        publish();

        threadPool.shutdown();
        threadPool.awaitTermination(2, TimeUnit.SECONDS);
        assertArrayEquals(messages.toArray(), messagesDeque.toArray());
        assertTrue("Invalid channel", channelsDeque.stream().allMatch(channel -> channel.equals(KEY)));
    }

    private void init(AllowableValue queueMode) {
        testRunner.setProperty(PublishRedis.REDIS_CONNECTION_POOL, "redis-connection-pool");
        testRunner.setProperty(PublishRedis.QUEUE_MODE, queueMode);
        testRunner.setProperty(PublishRedis.CHANNEL_OR_LIST, KEY);
    }

    private void publish() {
        // queue a flow file to trigger the processor and executeProcessor it
        messages.forEach(payload -> testRunner.enqueue(payload));
        testRunner.run(messages.size());
        testRunner.assertAllFlowFilesTransferred(PublishRedis.REL_SUCCESS, messages.size());
    }
}
