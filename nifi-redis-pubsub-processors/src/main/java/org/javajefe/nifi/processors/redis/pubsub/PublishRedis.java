package org.javajefe.nifi.processors.redis.pubsub;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.redis.RedisConnectionPool;
import org.javajefe.nifi.processors.redis.pubsub.util.RedisAction;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by Alexander Bukarev on 14.11.2018.
 */
//@SupportsBatching
@SeeAlso({ SubscribeRedis.class })
@Tags({ "Redis", "PubSub", "Queue" })
@CapabilityDescription("PUBLISH, LPUSH, RPUSH commands support to emulate a queue using Redis.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PublishRedis extends AbstractRedisProcessor {

    public static final AllowableValue PUBLISH_MODE = new AllowableValue("PUBLISH");
    public static final AllowableValue LPUSH_MODE = new AllowableValue("LPUSH");
    public static final AllowableValue RPUSH_MODE = new AllowableValue("RPUSH");
    public static final PropertyDescriptor QUEUE_MODE = new PropertyDescriptor.Builder()
            .name("Queue Mode")
            .description("Queue implementation mode (Pub/Sub or List implementation)")
            .required(true)
            .allowableValues(PUBLISH_MODE, LPUSH_MODE, RPUSH_MODE)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor CHANNEL_OR_LIST = new PropertyDescriptor.Builder()
            .name("Channel/Key Name")
            .description("Channel name (for PUBLISH mode) or Key name the list is stored (for LPUSH and RPUSH modes)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException, IOException {

        final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(100, DataUnit.KB, 10000));
        if (flowFiles.isEmpty()) {
            return;
        }

        // Send each FlowFile to Redis
        byte[] channelOrKeyBytes = channelOrKey.getBytes(StandardCharsets.UTF_8);

        switch (mode) {
            case "PUBLISH":
                final Iterator<FlowFile> itr = flowFiles.iterator();
                while (itr.hasNext()) {
                    final FlowFile flowFile = itr.next();

                    if (!isScheduled()) {
                        // If stopped, re-queue FlowFile instead of sending it
                        session.transfer(flowFile);
                        continue;
                    }

                    executePublish(session, channelOrKeyBytes, Collections.singletonList(flowFile));
                }
                break;
            default:
                executePublish(session, channelOrKeyBytes, flowFiles);
                break;
        }
	}

	private byte[] serializeFlowFile(ProcessSession session, FlowFile flowFile) {
        byte[] result = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(flowFile, baos);
        byte[] flowFileContent = baos.toByteArray();
        result = flowFileContent;
        return result;
    }

	private void executePublish(ProcessSession session, byte[] channelOrKeyBytes, List<FlowFile> flowFiles) {
        final long start = System.nanoTime();
        byte[][] flowFilesContent = flowFiles.stream()
                .map(flowFile -> serializeFlowFile(session, flowFile))
                .toArray(byte[][]::new);

        try {
            final RedisAction<Void> action = redisConnection -> {
                switch (mode) {
                    case "PUBLISH":
                        if (flowFilesContent.length != 1) {
                            throw new IllegalStateException("PUBLISH command accepts only one value");
                        }
                        redisConnection.publish(channelOrKeyBytes, flowFilesContent[0]);
                        break;
                    case "LPUSH":
                        redisConnection.listCommands().lPush(channelOrKeyBytes, flowFilesContent);
                        break;
                    case "RPUSH":
                        redisConnection.listCommands().rPush(channelOrKeyBytes, flowFilesContent);
                        break;
                    default:
                        throw new UnsupportedOperationException("Queue mode " + mode + " is not supported");
                }
                return null;
            };
            withConnection(action::execute);
            final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            for (FlowFile flowFile: flowFiles) {
                getLogger().debug("Successfully published message to Redis for {}", new Object[]{flowFile});
                session.getProvenanceReporter().send(flowFile, channelOrKey, transmissionMillis);
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (Exception ex) {
            for (FlowFile flowFile: flowFiles) {
                getLogger().info("Failed to publish message to Redis for {}", new Object[]{flowFile});
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

	@OnScheduled
    public void startPublishing(final ProcessContext context) {
        redisConnectionPool = context.getProperty(REDIS_CONNECTION_POOL)
                .asControllerService(RedisConnectionPool.class);
        mode = context.getProperty(QUEUE_MODE).getValue();
        channelOrKey = context.getProperty(CHANNEL_OR_LIST).evaluateAttributeExpressions().getValue();
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
