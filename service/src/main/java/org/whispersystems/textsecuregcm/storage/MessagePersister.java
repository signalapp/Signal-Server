package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class MessagePersister implements Managed {

    private final MessagesCache   messagesCache;
    private final MessagesManager messagesManager;
    private final AccountsManager accountsManager;

    private final Duration persistDelay;

    private volatile boolean running = false;
    private          Thread  workerThread;

    private final MetricRegistry metricRegistry         = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
    private final Timer          getQueuesTimer         = metricRegistry.timer(name(MessagePersister.class, "getQueues"));
    private final Timer          persistQueueTimer      = metricRegistry.timer(name(MessagePersister.class, "persistQueue"));
    private final Histogram      queueCountHistogram    = metricRegistry.histogram(name(MessagePersister.class, "queueCount"));
    private final Histogram      queueSizeHistogram     = metricRegistry.histogram(name(MessagePersister.class, "queueSize"));

    static final int QUEUE_BATCH_LIMIT   = 100;
    static final int MESSAGE_BATCH_LIMIT = 100;

    static final String ENABLE_PERSISTENCE_FLAG = "enable-cluster-persister";

    private static final Logger logger = LoggerFactory.getLogger(MessagePersister.class);

    public MessagePersister(final MessagesCache messagesCache, final MessagesManager messagesManager, final AccountsManager accountsManager, final Duration persistDelay) {
        this.messagesCache       = messagesCache;
        this.messagesManager     = messagesManager;
        this.accountsManager     = accountsManager;
        this.persistDelay        = persistDelay;
    }

    @VisibleForTesting
    Duration getPersistDelay() {
        return persistDelay;
    }

    @Override
    public void start() {
        running = true;

        workerThread = new Thread(() -> {
            while (running) {
                persistNextQueues(Instant.now());
                Util.sleep(100);
            }
        });

        workerThread.start();
    }

    @Override
    public void stop() throws Exception {
        running = false;

        if (workerThread != null) {
            workerThread.join();
            workerThread = null;
        }
    }

    @VisibleForTesting
    void persistNextQueues(final Instant currentTime) {
        final int slot = messagesCache.getNextSlotToPersist();

        List<String> queuesToPersist;
        int queuesPersisted = 0;

        do {
            try (final Timer.Context ignored = getQueuesTimer.time()) {
                queuesToPersist = messagesCache.getQueuesToPersist(slot, currentTime.minus(persistDelay), QUEUE_BATCH_LIMIT);
            }

            for (final String queue : queuesToPersist) {
                persistQueue(queue);
            }

            queuesPersisted += queuesToPersist.size();
        } while (queuesToPersist.size() == QUEUE_BATCH_LIMIT);

        queueCountHistogram.update(queuesPersisted);
    }

    @VisibleForTesting
    void persistQueue(final String queue) {
        final UUID accountUuid = MessagesCache.getAccountUuidFromQueueName(queue);
        final long deviceId    = MessagesCache.getDeviceIdFromQueueName(queue);

        final Optional<Account> maybeAccount = accountsManager.get(accountUuid);

        final String accountNumber;

        if (maybeAccount.isPresent()) {
            accountNumber = maybeAccount.get().getNumber();
        } else {
            logger.error("No account record found for account {}", accountUuid);
            return;
        }

        try (final Timer.Context ignored = persistQueueTimer.time()) {
            messagesCache.lockQueueForPersistence(queue);

            try {
                int messageCount = 0;
                List<MessageProtos.Envelope> messages;

                do {
                    messages = messagesCache.getMessagesToPersist(accountUuid, deviceId, MESSAGE_BATCH_LIMIT);

                    for (final MessageProtos.Envelope message : messages) {
                        messagesManager.persistMessage(accountNumber, accountUuid, message, UUID.fromString(message.getServerGuid()), deviceId);
                        messageCount++;
                    }
                } while (!messages.isEmpty());

                queueSizeHistogram.update(messageCount);
            } finally {
                messagesCache.unlockQueueForPersistence(queue);
            }
        }
    }
}
