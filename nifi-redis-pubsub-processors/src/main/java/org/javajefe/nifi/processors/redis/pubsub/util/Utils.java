package org.javajefe.nifi.processors.redis.pubsub.util;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.javajefe.nifi.processors.redis.pubsub.AbstractRedisProcessor;

import java.io.BufferedOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Created by Alexander Bukarev on 15.11.2018.
 */
public class Utils {

    public static <T> T waitForObject(Supplier<T> action, int stepDelay, Runnable loggingAction) {
        T result = null;
        int i = 0;
        while (result == null) {
            result = action.get();
            if (result == null) {
                try {
                    Thread.sleep(stepDelay);
                    if (i++ % (1000 / stepDelay) == 0) {
                        loggingAction.run();
                    }
                } catch (final InterruptedException e) {
                }
            }
        }
        return result;
    }

    public static boolean receiveMessage(final ProcessSession session, ComponentLog logger, String clientName,
                                      String channel, byte[] message, long startNanos) {
        if (message == null) {
            return false;
        }
        FlowFile flowFile = session.create();

        // TODO Think about attributes
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("channel", channel);
        attributes.put("clientName", clientName);
        flowFile = session.putAllAttributes(flowFile, attributes);

        flowFile = session.write(flowFile, out -> {
            try (final BufferedOutputStream bos = new BufferedOutputStream(out, 65536)) {
                bos.write(message);
                bos.flush();
            }
        });
        logger.info("Message received from Redis for {}", new Object[]{flowFile});
        final long transferNanos = System.nanoTime() - startNanos;
        final long transferMillis = TimeUnit.MILLISECONDS.convert(transferNanos, TimeUnit.NANOSECONDS);
        session.getProvenanceReporter().receive(flowFile,
                channel,
                clientName,
                "",
                transferMillis);

        session.transfer(flowFile, AbstractRedisProcessor.REL_SUCCESS);
        session.commit();
        return true;
    }
}
