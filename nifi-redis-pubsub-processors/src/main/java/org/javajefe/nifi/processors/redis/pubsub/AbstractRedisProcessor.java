package org.javajefe.nifi.processors.redis.pubsub;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.redis.RedisConnectionPool;
import org.springframework.data.redis.connection.RedisConnection;
import org.javajefe.nifi.processors.redis.pubsub.util.RedisAction;

import java.io.IOException;
import java.util.*;

/**
 * Abstract class for all Redis processors.
 * Created by Alexander Bukarev on 14.11.2018.
 */
public abstract class AbstractRedisProcessor  extends AbstractSessionFactoryProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("FlowFiles are routed to success relationship").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("FlowFiles are routed to failure relationship").build();
    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("redis-connection-pool")
            .displayName("Redis Connection Pool")
            .identifiesControllerService(RedisConnectionPool.class)
            .required(true)
            .build();

    protected volatile RedisConnectionPool redisConnectionPool;
    protected volatile String mode;
    protected volatile String channelOrKey;
    private List<PropertyDescriptor> descriptors;

    protected abstract PropertyDescriptor getQueueModePropertyDescriptor();

    protected abstract PropertyDescriptor getChannelOrKeyPropertyDescriptor();

    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(REDIS_CONNECTION_POOL);
        descriptors.add(getQueueModePropertyDescriptor());
        descriptors.add(getChannelOrKeyPropertyDescriptor());
        this.descriptors = Collections.unmodifiableList(descriptors);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory)
            throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger(context, session);
            session.commit();
        } catch (ProcessException t) {
            session.rollback(true);
            throw t;
        } catch (IOException e) {
            session.rollback(true);
            throw new ProcessException(e);
        }
    }

    public abstract void onTrigger(ProcessContext context, ProcessSession session)
            throws ProcessException, IOException;

    protected  <T> T withConnection(final RedisAction<T> action) throws IOException {
        RedisConnection redisConnection = null;
        try {
            redisConnection = redisConnectionPool.getConnection();
            return action.execute(redisConnection);
        } finally {
            if (redisConnection != null) {
                try {
                    redisConnection.close();
                } catch (Exception e) {
                    getLogger().warn("Error closing connection: " + e.getMessage(), e);
                }
            }
        }
    }
}
