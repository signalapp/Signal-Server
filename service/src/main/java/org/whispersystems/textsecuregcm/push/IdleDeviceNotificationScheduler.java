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
  record JobDescriptor(UUID accountIdentifier, byte deviceId, long lastSeen) {}

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
    final JobDescriptor jobDescriptor;

    try {
      jobDescriptor = SystemMapper.jsonMapper().readValue(jobData, JobDescriptor.class);
    } catch (final IOException e) {
      return CompletableFuture.failedFuture(e);
    }

    return accountsManager.getByAccountIdentifierAsync(jobDescriptor.accountIdentifier())
        .thenCompose(maybeAccount -> maybeAccount.map(account ->
                account.getDevice(jobDescriptor.deviceId()).map(device -> {
                      if (jobDescriptor.lastSeen() != device.getLastSeen()) {
                        return CompletableFuture.completedFuture("deviceSeenRecently");
                      }

                      try {
                        return pushNotificationManager
                            .sendNewMessageNotification(account, jobDescriptor.deviceId(), true)
                            .thenApply(ignored -> "sent");
                      } catch (final NotPushRegisteredException e) {
                        return CompletableFuture.completedFuture("deviceTokenDeleted");
                      }
                    })
                    .orElse(CompletableFuture.completedFuture("deviceDeleted")))
            .orElse(CompletableFuture.completedFuture("accountDeleted")));
  }

  public CompletableFuture<Void> scheduleNotification(final Account account, final Device device, final LocalTime preferredDeliveryTime) {
    final Instant runAt = SchedulingUtil.getNextRecommendedNotificationTime(account, preferredDeliveryTime, clock);

    try {
      return scheduleJob(runAt, SystemMapper.jsonMapper().writeValueAsBytes(
          new JobDescriptor(account.getIdentifier(IdentityType.ACI), device.getId(), device.getLastSeen())));
    } catch (final JsonProcessingException e) {
      // This should never happen when serializing an `AccountAndDeviceIdentifier`
      throw new AssertionError(e);
    }
  }
}
