package org.whispersystems.textsecuregcm.workers;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.ScoredValue;
import net.sourceforge.argparse4j.inf.Namespace;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.configuration.DatabaseConfiguration;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.Messages;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

public class ScourMessageCacheCommand extends ConfiguredCommand<WhisperServerConfiguration> {

    private FaultTolerantRedisClient redisClient;
    private Messages                 messageDatabase;

    private static final int MESSAGE_PAGE_SIZE = 100;

    private static final Logger log = LoggerFactory.getLogger(ScourMessageCacheCommand.class);

    public ScourMessageCacheCommand() {
        super("scourmessagecache", "Persist and remove all message queues from the old message cache");
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    protected void run(final Bootstrap<WhisperServerConfiguration> bootstrap, final Namespace namespace, final WhisperServerConfiguration config) {
        final DatabaseConfiguration messageDbConfig = config.getMessageStoreConfiguration();
        final Jdbi messageJdbi                      = Jdbi.create(messageDbConfig.getUrl(), messageDbConfig.getUser(), messageDbConfig.getPassword());
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

        final long          deviceId      = Long.parseLong(queueKey.substring(queueKey.lastIndexOf("::") + 2));
        final byte[]        queueKeyBytes = queueKey.getBytes(StandardCharsets.UTF_8);

        int                       messageCount  = 0;
        List<ScoredValue<byte[]>> messages;

        do {
            final int start = messageCount;

            messages = redisClient.withBinaryClient(connection -> connection.sync().zrangeWithScores(queueKeyBytes, start, start + MESSAGE_PAGE_SIZE));

            for (final ScoredValue<byte[]> scoredValue : messages) {
                persistMessage(accountNumber, deviceId, scoredValue.getValue());
                messageCount++;
            }
        } while (!messages.isEmpty());

        redisClient.useClient(connection -> {
            final String accountNumberAndDeviceId = accountNumber + "::" + deviceId;

            connection.async().del("user_queue::" + accountNumberAndDeviceId,
                                  "user_queue_metadata::" + accountNumberAndDeviceId,
                                  "user_queue_persisting::" + accountNumberAndDeviceId);
        });

        log.info("Persisted a queue with {} messages", messageCount);
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
