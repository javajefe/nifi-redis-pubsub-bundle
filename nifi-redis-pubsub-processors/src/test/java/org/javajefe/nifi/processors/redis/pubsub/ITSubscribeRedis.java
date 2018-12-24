package org.javajefe.nifi.processors.redis.pubsub;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * This is an integration test that is meant to be run against a real Redis instance.
 */
public class ITSubscribeRedis extends ITAbstractRedis {

    @Before
    public void setup() throws IOException, InitializationException {
        setup(new SubscribeRedis());
    }

    @After
    public void teardown() throws IOException {
        super.teardown();
    }

    @Test
    public void testLPOP() {
        generateMessages(10);
        messages.forEach(payload -> jedis.rpush(KEY, payload));
        init(SubscribeRedis.LPOP_MODE);
        subscribe();
    }

    @Test
    public void testRPOP() {
        generateMessages(10);
        messages.forEach(payload -> jedis.lpush(KEY, payload));
        init(SubscribeRedis.RPOP_MODE);
        subscribe();
    }

    @Test
    public void testSUBSCRIBE() throws InterruptedException {
        generateMessages(10);
        init(SubscribeRedis.SUBSCRIBE_MODE);

        testRunner.setRunSchedule(500);
        testRunner.run(2, false, true);
        messages.forEach(payload -> jedis.publish(KEY, payload));
        testRunner.run(1, true, false);

        List<MockFlowFile> result = testRunner.getFlowFilesForRelationship(SubscribeRedis.REL_SUCCESS);
        assertEquals(messages.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            result.get(i).assertContentEquals(messages.get(i));
        }
    }

    private void init(AllowableValue queueMode) {
        testRunner.setProperty(SubscribeRedis.REDIS_CONNECTION_POOL, "redis-connection-pool");
        testRunner.setProperty(SubscribeRedis.QUEUE_MODE, queueMode);
        testRunner.setProperty(SubscribeRedis.CHANNEL_OR_LIST, KEY);
    }

    private void subscribe() {
        testRunner.run(messages.size() + 1, true);
        List<MockFlowFile> result = testRunner.getFlowFilesForRelationship(SubscribeRedis.REL_SUCCESS);
        assertEquals(messages.size(), result.size());
        for (int i = 0; i < result.size(); i++) {
            result.get(i).assertContentEquals(messages.get(i));
        }
    }
}
