package org.javajefe.nifi.processors.redis.pubsub;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.lang.Nullable;
import org.javajefe.nifi.processors.redis.pubsub.util.Utils;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Alexander Bukarev on 15.11.2018.
 */
public class DefaultRedisSubscriber implements MessageListener {

    private final ComponentLog logger;
    private final String clientName;
    private volatile ProcessSession session;

    public DefaultRedisSubscriber(ComponentLog logger, String clientName) {
        this.logger = logger;
        this.clientName = clientName;
    }

    public void initializeSubscription(RedisConnection redisConnection, String channel,
                                       AtomicReference<ProcessSessionFactory> sessionFactoryReference) {
        // block until we have a session factory (occurs when processor is triggered)
        final ProcessSessionFactory sessionFactory = Utils.waitForObject(sessionFactoryReference::get,
                100,
                () -> logger.info("Waiting for sessionFactory"));
        session = sessionFactory.createSession();
        redisConnection.subscribe(this, channel.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void onMessage(Message message, @Nullable byte[] pattern) {
        final long startNanos = System.nanoTime();
        final String channel = new String(message.getChannel(), StandardCharsets.UTF_8);
        Utils.receiveMessage(session, logger, clientName, channel, message.getBody(), startNanos);
    }
}
