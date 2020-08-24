package org.whispersystems.textsecuregcm.workers;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jdbi3.strategies.DefaultNameStrategy;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Environment;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import net.sourceforge.argparse4j.inf.Namespace;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.Messages;
import org.whispersystems.textsecuregcm.util.Constants;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;

public class ScourMessageCacheCommand extends EnvironmentCommand<WhisperServerConfiguration> {

    private FaultTolerantRedisClient redisClient;
    private Messages                 messageDatabase;

    private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
    private final Histogram queueSizeHistogram  = metricRegistry.histogram(name(getClass(), "queueSize"));

    private static final Logger log = LoggerFactory.getLogger(ScourMessageCacheCommand.class);

    public ScourMessageCacheCommand() {
        super(new Application<>() {
            @Override
            public void run(final WhisperServerConfiguration whisperServerConfiguration, final Environment environment) {
            }
        }, "scourmessagecache", "Persist and remove all message queues from the old message cache");
    }

    @Override
    protected void run(final Environment environment, final Namespace namespace, final WhisperServerConfiguration config) {
        final JdbiFactory jdbiFactory               = new JdbiFactory(DefaultNameStrategy.CHECK_EMPTY);
        final Jdbi messageJdbi                      = jdbiFactory.build(environment, config.getMessageStoreConfiguration(), "messagedb" );
        final FaultTolerantDatabase messageDatabase = new FaultTolerantDatabase("message_database", messageJdbi, config.getMessageStoreConfiguration().getCircuitBreakerConfiguration());

        this.setMessageDatabase(new Messages(messageDatabase));
        this.setRedisClient(new FaultTolerantRedisClient("scourMessageCacheClient", config.getMessageCacheConfiguration().getRedisConfiguration()));

        scourMessageCache();
    }

    @VisibleForTesting
    void setRedisClient(final FaultTolerantRedisClient redisClient) {
        this.redisClient = redisClient;
    }

    @VisibleForTesting
    void setMessageDatabase(final Messages messageDatabase) {
        this.messageDatabase = messageDatabase;
    }

    @VisibleForTesting
    void scourMessageCache() {
        redisClient.useClient(connection -> ScanIterator.scan(connection.sync(), ScanArgs.Builder.matches("user_queue::*"))
                                                        .stream()
                                                        .forEach(this::persistQueue));
    }

    @VisibleForTesting
    void persistQueue(final String queueKey) {
        final String accountNumber;
        {
            final int startOfAccountNumber = queueKey.indexOf("::");
            accountNumber = queueKey.substring(startOfAccountNumber + 2, queueKey.indexOf("::", startOfAccountNumber + 1));
        }

        final long deviceId = Long.parseLong(queueKey.substring(queueKey.lastIndexOf("::") + 2));

        final AtomicInteger messageCount = new AtomicInteger(0);

        redisClient.useBinaryClient(connection -> connection.sync().zrange(messageBytes -> {
            persistMessage(accountNumber, deviceId, messageBytes);
            messageCount.incrementAndGet();
        }, queueKey.getBytes(StandardCharsets.UTF_8), 0, Long.MAX_VALUE));

        redisClient.useClient(connection -> {
            final String accountNumberAndDeviceId = accountNumber + "::" + deviceId;

            connection.async().del("user_queue::" + accountNumberAndDeviceId,
                                  "user_queue_metadata::" + accountNumberAndDeviceId,
                                  "user_queue_persisting::" + accountNumberAndDeviceId);
        });

        queueSizeHistogram.update(messageCount.longValue());
    }

    private void persistMessage(final String accountNumber, final long deviceId, final byte[] message) {
        try {
            MessageProtos.Envelope envelope = MessageProtos.Envelope.parseFrom(message);
            UUID                   guid     = envelope.hasServerGuid() ? UUID.fromString(envelope.getServerGuid()) : null;

            envelope = envelope.toBuilder().clearServerGuid().build();

            messageDatabase.store(guid, envelope, accountNumber, deviceId);
        } catch (InvalidProtocolBufferException e) {
            log.error("Error parsing envelope", e);
        }
    }
}
