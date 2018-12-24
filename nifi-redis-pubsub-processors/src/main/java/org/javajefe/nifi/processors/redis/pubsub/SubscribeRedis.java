package org.javajefe.nifi.processors.redis.pubsub;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.redis.RedisConnectionPool;
import org.springframework.data.redis.connection.RedisConnection;
import org.javajefe.nifi.processors.redis.pubsub.util.RedisAction;
import org.javajefe.nifi.processors.redis.pubsub.util.Utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Alexander Bukarev on 14.11.2018.
 */
//@SupportsBatching
@TriggerSerially
@SeeAlso({ PublishRedis.class })
@Tags({ "Redis", "PubSub", "Queue" })
@CapabilityDescription("SUBSCRIBE, LPOP, RPOP commands support to emulate a queue using Redis.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class SubscribeRedis extends AbstractRedisProcessor {

    public static final AllowableValue SUBSCRIBE_MODE = new AllowableValue("SUBSCRIBE");
    public static final AllowableValue LPOP_MODE = new AllowableValue("LPOP");
    public static final AllowableValue RPOP_MODE = new AllowableValue("RPOP");
    public static final PropertyDescriptor QUEUE_MODE = new PropertyDescriptor.Builder()
            .name("Queue Mode")
            .description("Queue implementation mode (Pub/Sub or List implementation)")
            .required(true)
            .allowableValues(SUBSCRIBE_MODE, LPOP_MODE, RPOP_MODE)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor CHANNEL_OR_LIST = new PropertyDescriptor.Builder()
            .name("Channel/Key Name")
            .description("Channel name (for SUBSCRIBE mode) or Key name the list is stored (for LPOP and RPOP modes)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference = new AtomicReference<>();
    private volatile ExecutorService subscribers;
    private volatile RedisConnection redisConnection;

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        sessionFactoryReference.compareAndSet(null, sessionFactory);
        final String mode = context.getProperty(QUEUE_MODE).getValue();
        if (mode.equals(SUBSCRIBE_MODE.getValue())) {
            context.yield();
        } else {
            super.onTrigger(context, sessionFactory);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException, IOException {
        final long start = System.nanoTime();
        final RedisAction<byte[]> action = redisConnection -> {
            byte[] message;
            switch (mode) {
                case "LPOP":
                    message = redisConnection.listCommands()
                            .lPop(channelOrKey.getBytes(StandardCharsets.UTF_8));
                    break;
                case "RPOP":
                    message = redisConnection.listCommands()
                            .rPop(channelOrKey.getBytes(StandardCharsets.UTF_8));
                    break;
                default:
                    throw new UnsupportedOperationException("Queue mode " + mode + " is not supported");
            }
            return message;
        };
        byte[] message = withConnection(action::execute);
        // TODO Change clientName attribute
        Utils.receiveMessage(session, getLogger(), "XXX", channelOrKey, message, start);
    }

    @OnScheduled()
    public void startSubscription(final ProcessContext context) {
        redisConnectionPool = context.getProperty(REDIS_CONNECTION_POOL)
                .asControllerService(RedisConnectionPool.class);
        mode = context.getProperty(QUEUE_MODE).getValue();
        channelOrKey = context.getProperty(CHANNEL_OR_LIST).evaluateAttributeExpressions().getValue();

        if (mode.equals(SUBSCRIBE_MODE.getValue())) {
            redisConnection = redisConnectionPool.getConnection();
            final DefaultRedisSubscriber redisSubscriber = new DefaultRedisSubscriber(getLogger(), redisConnection.getClientName());
            // Must run in a separate thread because jedis.subscribe is a blocking operation
            subscribers = Executors.newSingleThreadExecutor();
            subscribers.submit(() ->
                    redisSubscriber.initializeSubscription(redisConnection, channelOrKey, sessionFactoryReference));
        }
    }

    @OnStopped
    public void stopSubscription(final ProcessContext context) {
        if (redisConnection != null && redisConnection.isSubscribed()) {
            try {
                redisConnection.getSubscription().unsubscribe();
                subscribers.shutdown();
                subscribers.awaitTermination(5, TimeUnit.SECONDS);
                subscribers = null;
            } catch (InterruptedException e) {
                getLogger().warn("Unable to cleanly shutdown due to {}", new Object[]{e});
            }
        }
        sessionFactoryReference.set(null);
    }

    @Override
    protected PropertyDescriptor getQueueModePropertyDescriptor() {
        return QUEUE_MODE;
    }

    @Override
    protected PropertyDescriptor getChannelOrKeyPropertyDescriptor() {
        return CHANNEL_OR_LIST;
    }
}
