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
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.push.PushSender;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.websocket.WebsocketAddress;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class RedisClusterMessagePersister implements Managed {

    private final RedisClusterMessagesCache messagesCache;
    private final Messages                  messagesDatabase;
    private final PubSubManager             pubSubManager;
    private final PushSender                pushSender;
    private final AccountsManager           accountsManager;

    private final Duration persistDelay;

    private volatile boolean running = false;
    private          Thread  workerThread;

    private final MetricRegistry metricRegistry         = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
    private final Timer          getQueuesTimer         = metricRegistry.timer(name(RedisClusterMessagePersister.class, "getQueues"));
    private final Timer          persistQueueTimer      = metricRegistry.timer(name(RedisClusterMessagePersister.class, "persistQueue"));
    private final Timer          notifySubscribersTimer = metricRegistry.timer(name(RedisClusterMessagePersister.class, "notifySubscribers"));
    private final Histogram      queueCountHistogram    = metricRegistry.histogram(name(RedisClusterMessagePersister.class, "queueCount"));
    private final Histogram      queueSizeHistogram     = metricRegistry.histogram(name(RedisClusterMessagePersister.class, "queueSize"));

    static final int QUEUE_BATCH_LIMIT   = 100;
    static final int MESSAGE_BATCH_LIMIT = 100;

    private static final Logger logger = LoggerFactory.getLogger(RedisClusterMessagePersister.class);

    public RedisClusterMessagePersister(final RedisClusterMessagesCache messagesCache, final Messages messagesDatabase, final PubSubManager pubSubManager, final PushSender pushSender, final AccountsManager accountsManager, final Duration persistDelay) {
        this.messagesCache    = messagesCache;
        this.messagesDatabase = messagesDatabase;
        this.pubSubManager    = pubSubManager;
        this.pushSender       = pushSender;
        this.accountsManager  = accountsManager;

        this.persistDelay     = persistDelay;
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
                notifyClients(RedisClusterMessagesCache.getAccountUuidFromQueueName(queue), RedisClusterMessagesCache.getDeviceIdFromQueueName(queue));
            }

            queuesPersisted += queuesToPersist.size();
        } while (queuesToPersist.size() == QUEUE_BATCH_LIMIT);

        queueCountHistogram.update(queuesPersisted);
    }

    @VisibleForTesting
    void persistQueue(final String queue) {
        final UUID accountUuid = RedisClusterMessagesCache.getAccountUuidFromQueueName(queue);
        final long deviceId    = RedisClusterMessagesCache.getDeviceIdFromQueueName(queue);

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
                        final UUID uuid = UUID.fromString(message.getServerGuid());

                        messagesDatabase.store(uuid, message, accountNumber, deviceId);
                        messagesCache.remove(accountNumber, accountUuid, deviceId, uuid);

                        messageCount++;
                    }
                } while (messages.size() == MESSAGE_BATCH_LIMIT);

                queueSizeHistogram.update(messageCount);
            } finally {
                messagesCache.unlockQueueForPersistence(queue);
            }
        }
    }

    public void notifyClients(final UUID accountUuid, final long deviceId) {
        try (final Timer.Context ignored = notifySubscribersTimer.time()) {
            final Optional<Account> maybeAccount = accountsManager.get(accountUuid);

            final String address;

            if (maybeAccount.isPresent()) {
                address = maybeAccount.get().getNumber();
            } else {
                logger.error("No account record found for account {}", accountUuid);
                return;
            }

            final boolean notified = pubSubManager.publish(new WebsocketAddress(address, deviceId),
                    PubSubProtos.PubSubMessage.newBuilder()
                                              .setType(PubSubProtos.PubSubMessage.Type.QUERY_DB)
                                              .build());

            if (!notified) {
                Optional<Account> account = accountsManager.get(address);

                if (account.isPresent()) {
                    Optional<Device> device = account.get().getDevice(deviceId);

                    if (device.isPresent()) {
                        try {
                            pushSender.sendQueuedNotification(account.get(), device.get());
                        } catch (final NotPushRegisteredException e) {
                            logger.warn("After message persistence, no longer push registered!");
                        }
                    }
                }
            }
        }
    }
}
