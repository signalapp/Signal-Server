package org.whispersystems.textsecuregcm.push;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.scheduler.JobScheduler;
import org.whispersystems.textsecuregcm.scheduler.SchedulingUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class IdleDeviceNotificationScheduler extends JobScheduler {

  private final AccountsManager accountsManager;
  private final PushNotificationManager pushNotificationManager;
  private final Clock clock;

  @VisibleForTesting
  static final Duration MIN_IDLE_DURATION = Duration.ofDays(14);

  @VisibleForTesting
  record AccountAndDeviceIdentifier(UUID accountIdentifier, byte deviceId) {}

  public IdleDeviceNotificationScheduler(final AccountsManager accountsManager,
      final PushNotificationManager pushNotificationManager,
      final DynamoDbAsyncClient dynamoDbAsyncClient,
      final String tableName,
      final Duration jobExpiration,
      final Clock clock) {

    super(dynamoDbAsyncClient, tableName, jobExpiration, clock);

    this.accountsManager = accountsManager;
    this.pushNotificationManager = pushNotificationManager;
    this.clock = clock;
  }

  @Override
  public String getSchedulerName() {
    return "IdleDeviceNotification";
  }

  @Override
  protected CompletableFuture<String> processJob(@Nullable final byte[] jobData) {
    final AccountAndDeviceIdentifier accountAndDeviceIdentifier;

    try {
      accountAndDeviceIdentifier = SystemMapper.jsonMapper().readValue(jobData, AccountAndDeviceIdentifier.class);
    } catch (final IOException e) {
      return CompletableFuture.failedFuture(e);
    }

    return accountsManager.getByAccountIdentifierAsync(accountAndDeviceIdentifier.accountIdentifier())
        .thenCompose(maybeAccount -> maybeAccount.map(account ->
                account.getDevice(accountAndDeviceIdentifier.deviceId()).map(device -> {
                      if (!isIdle(device)) {
                        return CompletableFuture.completedFuture("deviceSeenRecently");
                      }

                      try {
                        return pushNotificationManager
                            .sendNewMessageNotification(account, accountAndDeviceIdentifier.deviceId(), true)
                            .thenApply(ignored -> "sent");
                      } catch (final NotPushRegisteredException e) {
                        return CompletableFuture.completedFuture("deviceTokenDeleted");
                      }
                    })
                    .orElse(CompletableFuture.completedFuture("deviceDeleted")))
            .orElse(CompletableFuture.completedFuture("accountDeleted")));
  }

  public boolean isIdle(final Device device) {
    final Duration idleDuration = Duration.between(Instant.ofEpochMilli(device.getLastSeen()), clock.instant());

    return idleDuration.compareTo(MIN_IDLE_DURATION) >= 0;
  }

  public CompletableFuture<Void> scheduleNotification(final Account account, final byte deviceId, final LocalTime preferredDeliveryTime) {
    final Instant runAt = SchedulingUtil.getNextRecommendedNotificationTime(account, preferredDeliveryTime, clock);

    try {
      return scheduleJob(runAt, SystemMapper.jsonMapper().writeValueAsBytes(
          new AccountAndDeviceIdentifier(account.getIdentifier(IdentityType.ACI), deviceId)));
    } catch (final JsonProcessingException e) {
      // This should never happen when serializing an `AccountAndDeviceIdentifier`
      throw new AssertionError(e);
    }
  }
}
