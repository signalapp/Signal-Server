/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;


import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.dropwizard.lifecycle.Managed;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisException;
import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.protocol.IdentityKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.DisconnectionRequestManager;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevices;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.DeviceInfo;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.RestoreAccountRequest;
import org.whispersystems.textsecuregcm.entities.TransferArchiveResult;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.redis.FaultTolerantPubSubConnection;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClient;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecoveryClient;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.RegistrationIdValidator;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

public class AccountsManager extends RedisPubSubAdapter<String, String> implements Managed {

  private static final Timer createTimer = Metrics.timer(name(AccountsManager.class, "create"));
  private static final Timer updateTimer = Metrics.timer(name(AccountsManager.class, "update"));
  private static final Timer getByNumberTimer = Metrics.timer(name(AccountsManager.class, "getByNumber"));
  private static final Timer getByUsernameHashTimer = Metrics.timer(name(AccountsManager.class, "getByUsernameHash"));
  private static final Timer getByUsernameLinkHandleTimer = Metrics.timer(name(AccountsManager.class, "getByUsernameLinkHandle"));
  private static final Timer getByUuidTimer = Metrics.timer(name(AccountsManager.class, "getByUuid"));
  private static final Timer deleteTimer = Metrics.timer(name(AccountsManager.class, "delete"));

  private static final Timer redisSetTimer = Metrics.timer(name(AccountsManager.class, "redisSet"));
  private static final Timer redisPniGetTimer = Metrics.timer(name(AccountsManager.class, "redisPniGet"));
  private static final Timer redisUuidGetTimer = Metrics.timer(name(AccountsManager.class, "redisUuidGet"));
  private static final Timer redisDeleteTimer = Metrics.timer(name(AccountsManager.class, "redisDelete"));

  private static final String CREATE_COUNTER_NAME       = name(AccountsManager.class, "createCounter");
  private static final String DELETE_COUNTER_NAME       = name(AccountsManager.class, "deleteCounter");
  private static final String COUNTRY_CODE_TAG_NAME     = "country";
  private static final String DELETION_REASON_TAG_NAME  = "reason";
  private static final String TIMESTAMP_BASED_TRANSFER_ARCHIVE_KEY_COUNTER_NAME = name(AccountsManager.class, "timestampRedisKeyCounter");
  private static final String REGISTRATION_ID_BASED_TRANSFER_ARCHIVE_KEY_COUNTER_NAME = name(AccountsManager.class,"registrationIdRedisKeyCounter");

  private static final String RETRY_NAME = ResilienceUtil.name(AccountsManager.class);

  private static final Duration SUBSCRIBE_RETRY_DELAY = Duration.ofSeconds(5);

  private static final Logger logger = LoggerFactory.getLogger(AccountsManager.class);

  private final Accounts accounts;
  private final PhoneNumberIdentifiers phoneNumberIdentifiers;
  private final FaultTolerantRedisClusterClient cacheCluster;
  private final FaultTolerantRedisClient pubSubRedisClient;
  private final AccountLockManager accountLockManager;
  private final KeysManager keysManager;
  private final MessagesManager messagesManager;
  private final ProfilesManager profilesManager;
  private final SecureStorageClient secureStorageClient;
  private final SecureValueRecoveryClient secureValueRecovery2Client;
  private final DisconnectionRequestManager disconnectionRequestManager;
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;
  private final ClientPublicKeysManager clientPublicKeysManager;
  private final Executor accountLockExecutor;
  private final ScheduledExecutorService messagesPollExecutor;
  private final ScheduledExecutorService retryExecutor;
  private final Clock clock;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  private final Key verificationTokenKey;

  private final FaultTolerantPubSubConnection<String, String> pubSubConnection;

  private final Map<String, CompletableFuture<Optional<DeviceInfo>>> waitForDeviceFuturesByTokenIdentifier =
      new ConcurrentHashMap<>();

  private final Map<DeviceIdentifier, CompletableFuture<Optional<TransferArchiveResult>>> waitForTransferArchiveFuturesByDeviceIdentifier =
      new ConcurrentHashMap<>();

  private final Map<String, CompletableFuture<Optional<RestoreAccountRequest>>> waitForRestoreAccountRequestFuturesByToken =
      new ConcurrentHashMap<>();

  private static final int SHA256_HASH_LENGTH = getSha256MessageDigest().getDigestLength();

  private static final Duration RECENTLY_ADDED_DEVICE_TTL = Duration.ofHours(1);
  private static final String LINKED_DEVICE_PREFIX = "linked_device::";
  private static final String LINKED_DEVICE_KEYSPACE_PATTERN = "__keyspace@0__:" + LINKED_DEVICE_PREFIX + "*";

  private static final Duration RECENTLY_ADDED_TRANSFER_ARCHIVE_TTL = Duration.ofHours(1);
  private static final String TRANSFER_ARCHIVE_PREFIX = "transfer_archive::";
  private static final String TRANSFER_ARCHIVE_KEYSPACE_PATTERN = "__keyspace@0__:" + TRANSFER_ARCHIVE_PREFIX + "*";
  private static final String TRANSFER_ARCHIVE_REGISTRATION_ID_PATTERN = "registrationId";

  private static final Duration RESTORE_ACCOUNT_REQUEST_TTL = Duration.ofHours(1);
  private static final String RESTORE_ACCOUNT_REQUEST_PREFIX = "restore_account::";
  private static final String RESTORE_ACCOUNT_REQUEST_KEYSPACE_PATTERN = "__keyspace@0__:" + RESTORE_ACCOUNT_REQUEST_PREFIX + "*";

  private static final ObjectWriter ACCOUNT_REDIS_JSON_WRITER = SystemMapper.jsonMapper()
      .writer(SystemMapper.excludingField(Account.class, List.of("uuid")));

  private static final Duration MESSAGE_POLL_INTERVAL = Duration.ofSeconds(1);
  private static final Duration MAX_SERVER_CLOCK_DRIFT = Duration.ofSeconds(5);

  // An account that's used at least daily will get reset in the cache at least once per day when its "last seen"
  // timestamp updates; expiring entries after two days will help clear out "zombie" cache entries that are read
  // frequently (e.g. the account is in an active group and receives messages frequently), but aren't actively used by
  // the owner.
  private static final long CACHE_TTL_SECONDS = Duration.ofDays(2).toSeconds();

  private static final Duration USERNAME_HASH_RESERVATION_TTL_MINUTES = Duration.ofMinutes(5);

  private static final int MAX_UPDATE_ATTEMPTS = 10;

  @VisibleForTesting
  static final Duration LINK_DEVICE_TOKEN_EXPIRATION_DURATION = Duration.ofMinutes(10);

  @VisibleForTesting
  static final String LINK_DEVICE_VERIFICATION_TOKEN_ALGORITHM = "HmacSHA256";

  public enum DeletionReason {
    ADMIN_DELETED("admin"),
    EXPIRED      ("expired"),
    USER_REQUEST ("userRequest");

    private final String tagValue;

    DeletionReason(final String tagValue) {
      this.tagValue = tagValue;
    }
  }

  private interface DeviceIdentifier {}

  private record TimestampDeviceIdentifier(UUID accountIdentifier, byte deviceId, Instant deviceCreationTimestamp)
      implements DeviceIdentifier {
  }

  private record RegistrationIdDeviceIdentifier(UUID accountIdentifier, byte deviceId,
                                                int registrationId) implements DeviceIdentifier {
  }

  public AccountsManager(final Accounts accounts,
      final PhoneNumberIdentifiers phoneNumberIdentifiers,
      final FaultTolerantRedisClusterClient cacheCluster,
      final FaultTolerantRedisClient pubSubRedisClient,
      final AccountLockManager accountLockManager,
      final KeysManager keysManager,
      final MessagesManager messagesManager,
      final ProfilesManager profilesManager,
      final SecureStorageClient secureStorageClient,
      final SecureValueRecoveryClient secureValueRecovery2Client,
      final DisconnectionRequestManager disconnectionRequestManager,
      final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
      final ClientPublicKeysManager clientPublicKeysManager,
      final Executor accountLockExecutor,
      final ScheduledExecutorService messagesPollExecutor, final ScheduledExecutorService retryExecutor,
      final Clock clock,
      final byte[] linkDeviceSecret,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this.accounts = accounts;
    this.phoneNumberIdentifiers = phoneNumberIdentifiers;
    this.cacheCluster = cacheCluster;
    this.pubSubRedisClient = pubSubRedisClient;
    this.accountLockManager = accountLockManager;
    this.keysManager = keysManager;
    this.messagesManager = messagesManager;
    this.profilesManager = profilesManager;
    this.secureStorageClient = secureStorageClient;
    this.secureValueRecovery2Client = secureValueRecovery2Client;
    this.disconnectionRequestManager = disconnectionRequestManager;
    this.registrationRecoveryPasswordsManager = requireNonNull(registrationRecoveryPasswordsManager);
    this.clientPublicKeysManager = clientPublicKeysManager;
    this.accountLockExecutor = accountLockExecutor;
    this.messagesPollExecutor = messagesPollExecutor;
    this.retryExecutor = retryExecutor;
    this.clock = requireNonNull(clock);
    this.dynamicConfigurationManager = dynamicConfigurationManager;

    this.verificationTokenKey = new SecretKeySpec(linkDeviceSecret, LINK_DEVICE_VERIFICATION_TOKEN_ALGORITHM);

    // Fail fast: reject bad keys
    try {
      getInitializedMac(verificationTokenKey);
    } catch (final InvalidKeyException e) {
      throw new IllegalArgumentException(e);
    }

    this.pubSubConnection = pubSubRedisClient.createPubSubConnection();
  }

  @Override
  public void start() {
    pubSubConnection.usePubSubConnection(connection -> {
      connection.addListener(this);

      boolean subscribed = false;

      // Loop indefinitely until we establish a subscription. We don't want to fail immediately if there's a temporary
      // Redis connectivity issue, since that would derail the whole startup process and likely lead to unnecessary pod
      // churn, which might make things worse. If we never establish a connection, readiness probes will eventually fail
      // and terminate the pods.
      do {
        try {
          connection.sync().psubscribe(LINKED_DEVICE_KEYSPACE_PATTERN, TRANSFER_ARCHIVE_KEYSPACE_PATTERN,
              RESTORE_ACCOUNT_REQUEST_KEYSPACE_PATTERN);

          subscribed = true;
        } catch (final RedisCommandTimeoutException e) {
          try {
            Thread.sleep(SUBSCRIBE_RETRY_DELAY);
          } catch (final InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        }
      } while (!subscribed);
    });
  }

  @Override
  public void stop() {
    pubSubConnection.usePubSubConnection(connection -> {
      connection.sync().punsubscribe();
      connection.removeListener(this);
    });
  }

  public Account create(final String number,
      final AccountAttributes accountAttributes,
      final List<AccountBadge> accountBadges,
      final IdentityKey aciIdentityKey,
      final IdentityKey pniIdentityKey,
      final DeviceSpec primaryDeviceSpec,
      @Nullable final String userAgent) throws InterruptedException {

    final UUID pni = phoneNumberIdentifiers.getPhoneNumberIdentifier(number).join();

    return createTimer.record(() -> {
      try {
        return accountLockManager.withLock(Set.of(pni),
            () -> create(number, pni, accountAttributes, accountBadges, aciIdentityKey, pniIdentityKey, primaryDeviceSpec, userAgent), accountLockExecutor);
      } catch (final Exception e) {
        if (e instanceof RuntimeException runtimeException) {
          throw runtimeException;
        }

        logger.error("Unexpected exception while creating account", e);
        throw new RuntimeException(e);
      }
    });
  }

  private Account create(final String number,
      final UUID pni,
      final AccountAttributes accountAttributes,
      final List<AccountBadge> accountBadges,
      final IdentityKey aciIdentityKey,
      final IdentityKey pniIdentityKey,
      final DeviceSpec primaryDeviceSpec,
      @Nullable final String userAgent) {

    final Account account = new Account();
    final Optional<UUID> maybeRecentlyDeletedAccountIdentifier =
        accounts.findRecentlyDeletedAccountIdentifier(pni);

    // Reuse the ACI from any recently-deleted account with this number to cover cases where somebody is
    // re-registering.
    account.setUuid(maybeRecentlyDeletedAccountIdentifier.orElseGet(UUID::randomUUID));
    account.setNumber(number, pni);
    account.setIdentityKey(aciIdentityKey);
    account.setPhoneNumberIdentityKey(pniIdentityKey);
    account.addDevice(primaryDeviceSpec.toDevice(Device.PRIMARY_ID, clock, aciIdentityKey));
    account.setRegistrationLockFromAttributes(accountAttributes);
    account.setUnidentifiedAccessKey(accountAttributes.getUnidentifiedAccessKey());
    account.setUnrestrictedUnidentifiedAccess(accountAttributes.isUnrestrictedUnidentifiedAccess());
    account.setDiscoverableByPhoneNumber(accountAttributes.isDiscoverableByPhoneNumber());
    account.setBadges(clock, accountBadges);

    String accountCreationType = maybeRecentlyDeletedAccountIdentifier.isPresent() ? "recently-deleted" : "new";

    final String pushTokenType;

    if (primaryDeviceSpec.apnRegistrationId().isPresent()) {
      pushTokenType = "apns";
    } else if (primaryDeviceSpec.gcmRegistrationId().isPresent()) {
      pushTokenType = "fcm";
    } else {
      pushTokenType = "none";
    }

    String previousPushTokenType = null;

    try {
      accounts.create(account, keysManager.buildWriteItemsForNewDevice(account.getIdentifier(IdentityType.ACI),
          account.getIdentifier(IdentityType.PNI),
          Device.PRIMARY_ID,
          primaryDeviceSpec.aciSignedPreKey(),
          primaryDeviceSpec.pniSignedPreKey(),
          primaryDeviceSpec.aciPqLastResortPreKey(),
          primaryDeviceSpec.pniPqLastResortPreKey()));
    } catch (final AccountAlreadyExistsException e) {
      accountCreationType = "re-registration";

      if (StringUtils.isNotBlank(e.getExistingAccount().getPrimaryDevice().getApnId())) {
        previousPushTokenType = "apns";
      } else if (StringUtils.isNotBlank(e.getExistingAccount().getPrimaryDevice().getGcmId())) {
        previousPushTokenType = "fcm";
      } else {
        previousPushTokenType = "none";
      }

      final UUID aci = e.getExistingAccount().getIdentifier(IdentityType.ACI);
      account.setUuid(aci);

      final List<TransactWriteItem> additionalWriteItems = Stream.concat(
              keysManager.buildWriteItemsForNewDevice(account.getIdentifier(IdentityType.ACI),
                  account.getIdentifier(IdentityType.PNI),
                  Device.PRIMARY_ID,
                  primaryDeviceSpec.aciSignedPreKey(),
                  primaryDeviceSpec.pniSignedPreKey(),
                  primaryDeviceSpec.aciPqLastResortPreKey(),
                  primaryDeviceSpec.pniPqLastResortPreKey()).stream(),
              e.getExistingAccount().getDevices()
                  .stream()
                  .map(Device::getId)
                  // No need to clear the keys for the primary device since we'll just overwrite them in the same
                  // transaction anyhow
                  .filter(existingDeviceId -> existingDeviceId != Device.PRIMARY_ID)
                  .flatMap(existingDeviceId ->
                      keysManager.buildWriteItemsForRemovedDevice(aci, pni, existingDeviceId).stream()))
          .toList();

      CompletableFuture.allOf(
              keysManager.deleteSingleUsePreKeys(aci),
              keysManager.deleteSingleUsePreKeys(pni),
              messagesManager.clear(aci),
              profilesManager.deleteAll(aci, false))
          .thenCompose(ignored -> disconnectionRequestManager.requestDisconnection(e.getExistingAccount()))
          .thenCompose(ignored -> accounts.reclaimAccount(e.getExistingAccount(), account, additionalWriteItems))
          .thenCompose(ignored -> {
            // We should have cleared all messages before overwriting the old account, but more may have arrived
            // while we were working. Similarly, the old account holder could have added keys or profiles. We'll
            // largely repeat the cleanup process after creating the account to make sure we really REALLY got
            // everything.
            //
            // We exclude the primary device's repeated-use keys from deletion because new keys were provided as
            // part of the account creation process, and we don't want to delete the keys that just got added.
            return CompletableFuture.allOf(keysManager.deleteSingleUsePreKeys(aci),
                keysManager.deleteSingleUsePreKeys(pni),
                messagesManager.clear(aci),
                profilesManager.deleteAll(aci, false));
          })
          .join();
    }

    redisSet(account);

    Tags tags = Tags.of(UserAgentTagUtil.getPlatformTag(userAgent),
        Tag.of("type", accountCreationType),
        Tag.of("hasPushToken", String.valueOf(
            primaryDeviceSpec.apnRegistrationId().isPresent() || primaryDeviceSpec.gcmRegistrationId()
                .isPresent())),
        Tag.of("pushTokenType", pushTokenType));

    if (StringUtils.isNotBlank(previousPushTokenType)) {
      tags = tags.and(Tag.of("previousPushTokenType", previousPushTokenType));
    }

    Metrics.counter(CREATE_COUNTER_NAME, tags).increment();

    accountAttributes.recoveryPassword().ifPresent(registrationRecoveryPassword ->
        registrationRecoveryPasswordsManager.store(account.getIdentifier(IdentityType.PNI),
            registrationRecoveryPassword));

    return account;
  }

  public CompletableFuture<Pair<Account, Device>> addDevice(final Account account, final DeviceSpec deviceSpec, final String linkDeviceToken) {
    return accountLockManager.withLockAsync(Set.of(account.getPhoneNumberIdentifier()),
        () -> addDevice(account.getIdentifier(IdentityType.ACI), deviceSpec, linkDeviceToken, MAX_UPDATE_ATTEMPTS),
        accountLockExecutor);
  }

  private CompletableFuture<Pair<Account, Device>> addDevice(final UUID accountIdentifier, final DeviceSpec deviceSpec, final String linkDeviceToken, final int retries) {
    return accounts.getByAccountIdentifierAsync(accountIdentifier)
        .thenApply(maybeAccount -> maybeAccount.orElseThrow(ContestedOptimisticLockException::new))
        .thenCompose(account -> {
          final byte nextDeviceId = account.getNextDeviceId();

          return CompletableFuture.allOf(
                  keysManager.deleteSingleUsePreKeys(account.getUuid(), nextDeviceId),
                  keysManager.deleteSingleUsePreKeys(account.getPhoneNumberIdentifier(), nextDeviceId),
                  messagesManager.clear(account.getUuid(), nextDeviceId))
              .thenApply(ignored -> new Pair<>(account, nextDeviceId));
        })
        .thenCompose(accountAndNextDeviceId -> {
          final Account account = accountAndNextDeviceId.first();
          final byte nextDeviceId = accountAndNextDeviceId.second();

          account.addDevice(deviceSpec.toDevice(nextDeviceId, clock, account.getIdentityKey(IdentityType.ACI)));

          final List<TransactWriteItem> additionalWriteItems = new ArrayList<>(keysManager.buildWriteItemsForNewDevice(
              account.getIdentifier(IdentityType.ACI),
              account.getIdentifier(IdentityType.PNI),
              nextDeviceId,
              deviceSpec.aciSignedPreKey(),
              deviceSpec.pniSignedPreKey(),
              deviceSpec.aciPqLastResortPreKey(),
              deviceSpec.pniPqLastResortPreKey()));

          additionalWriteItems.add(accounts.buildTransactWriteItemForLinkDevice(linkDeviceToken, LINK_DEVICE_TOKEN_EXPIRATION_DURATION));

          return accounts.updateTransactionallyAsync(account, additionalWriteItems)
              .thenApply(ignored -> new Pair<>(account, account.getDevice(nextDeviceId).orElseThrow()));
        })
        .thenCompose(updatedAccountAndDevice -> redisDeleteAsync(updatedAccountAndDevice.first())
            .thenApply(ignored -> updatedAccountAndDevice))
        .exceptionallyCompose(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof ContestedOptimisticLockException && retries > 0) {
            return addDevice(accountIdentifier, deviceSpec, linkDeviceToken, retries - 1);
          } else if (ExceptionUtils.unwrap(throwable) instanceof TransactionCanceledException transactionCanceledException) {
            // We can be confident the transaction was canceled because the linked device token was already used if the
            // "check token" transaction write item is the only one that failed. That SHOULD be the last one in the
            // list.
            final long cancelledTransactions = transactionCanceledException.cancellationReasons().stream()
                .filter(cancellationReason -> !"None".equals(cancellationReason.code()))
                .count();

            final boolean tokenReuseConditionFailed =
                "ConditionalCheckFailed".equals(transactionCanceledException.cancellationReasons().getLast().code());

            if (cancelledTransactions == 1 && tokenReuseConditionFailed) {
              return CompletableFuture.failedFuture(new LinkDeviceTokenAlreadyUsedException());
            }
          }

          return CompletableFuture.failedFuture(throwable);
        })
        .whenComplete((updatedAccountAndDevice, _) -> {
          if (updatedAccountAndDevice != null) {
            final String key = getLinkedDeviceKey(getLinkDeviceTokenIdentifier(linkDeviceToken));
            final String deviceInfoJson;

            try {
              deviceInfoJson = SystemMapper.jsonMapper().writeValueAsString(DeviceInfo.forDevice(updatedAccountAndDevice.second()));
            } catch (final JsonProcessingException e) {
              throw new UncheckedIOException(e);
            }

            ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
                .executeCompletionStage(retryExecutor, () -> pubSubRedisClient.withConnection(connection ->
                    connection.async().set(key, deviceInfoJson, SetArgs.Builder.ex(RECENTLY_ADDED_DEVICE_TTL))))
                .whenComplete((_, pubSubThrowable) -> {
                  if (pubSubThrowable != null) {
                    logger.warn("Failed to record recently-created device", pubSubThrowable);
                  }
                });
          }
        });
  }

  private Mac getInitializedMac() {
    try {
      return getInitializedMac(verificationTokenKey);
    } catch (final InvalidKeyException e) {
      // We checked the key at construction time, so this can never happen
      throw new AssertionError("Previously valid key now invalid", e);
    }
  }

  private static Mac getInitializedMac(final Key linkDeviceTokenKey) throws InvalidKeyException {
    try {
      final Mac mac = Mac.getInstance(LINK_DEVICE_VERIFICATION_TOKEN_ALGORITHM);
      mac.init(linkDeviceTokenKey);

      return mac;
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  public String generateLinkDeviceToken(final UUID aci) {
    final String claims = aci + "." + clock.instant().toEpochMilli();
    final byte[] signature = getInitializedMac().doFinal(claims.getBytes(StandardCharsets.UTF_8));

    return claims + ":" + Base64.getUrlEncoder().encodeToString(signature);
  }

  @VisibleForTesting
  static String generateLinkDeviceToken(final UUID aci, final Key linkDeviceTokenKey, final Clock clock)
      throws InvalidKeyException {

    final String claims = aci + "." + clock.instant().toEpochMilli();
    final byte[] signature = getInitializedMac(linkDeviceTokenKey).doFinal(claims.getBytes(StandardCharsets.UTF_8));

    return claims + ":" + Base64.getUrlEncoder().encodeToString(signature);
  }

  public static String getLinkDeviceTokenIdentifier(final String linkDeviceToken) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(
        getSha256MessageDigest().digest(linkDeviceToken.getBytes(StandardCharsets.UTF_8)));
  }

  /**
   * Checks that a device-linking token is valid and returns the account identifier from the token if so, or empty if
   * the token was invalid
   *
   * @param token the device-linking token to check
   *
   * @return the account identifier from a valid token or empty if the token was invalid
   */
  public Optional<UUID> checkDeviceLinkingToken(final String token) {
    final String[] claimsAndSignature = token.split(":", 2);

    if (claimsAndSignature.length != 2) {
      return Optional.empty();
    }

    final byte[] expectedSignature = getInitializedMac().doFinal(claimsAndSignature[0].getBytes(StandardCharsets.UTF_8));
    final byte[] providedSignature;

    try {
      providedSignature = Base64.getUrlDecoder().decode(claimsAndSignature[1]);
    } catch (final IllegalArgumentException e) {
      return Optional.empty();
    }

    if (!MessageDigest.isEqual(expectedSignature, providedSignature)) {
      return Optional.empty();
    }

    final String[] aciAndTimestamp = claimsAndSignature[0].split("\\.", 2);

    if (aciAndTimestamp.length != 2) {
      return Optional.empty();
    }

    final UUID aci;

    try {
      aci = UUID.fromString(aciAndTimestamp[0]);
    } catch (final IllegalArgumentException e) {
      return Optional.empty();
    }

    final Instant timestamp;

    try {
      timestamp = Instant.ofEpochMilli(Long.parseLong(aciAndTimestamp[1]));
    } catch (final NumberFormatException e) {
      return Optional.empty();
    }

    final Instant tokenExpiration = timestamp.plus(LINK_DEVICE_TOKEN_EXPIRATION_DURATION);

    if (tokenExpiration.isBefore(clock.instant())) {
      return Optional.empty();
    }

    return Optional.of(aci);
  }

  /**
   * Unlink a device from the given account. The device will be immediately disconnected if it is connected to any chat
   * frontend.
   *
   * @return the updated Account
   */
  public CompletableFuture<Account> removeDevice(final Account account, final byte deviceId) {
    if (deviceId == Device.PRIMARY_ID) {
      throw new IllegalArgumentException("Cannot remove primary device");
    }

    return accountLockManager.withLockAsync(Set.of(account.getPhoneNumberIdentifier()),
        () -> removeDevice(account.getIdentifier(IdentityType.ACI), deviceId, MAX_UPDATE_ATTEMPTS),
        accountLockExecutor);
  }

  private CompletableFuture<Account> removeDevice(final UUID accountIdentifier, final byte deviceId, final int retries) {
    return accounts.getByAccountIdentifierAsync(accountIdentifier)
        .thenApply(maybeAccount -> maybeAccount.orElseThrow(ContestedOptimisticLockException::new))
        .thenCompose(account ->  CompletableFuture.allOf(
            keysManager.deleteSingleUsePreKeys(account.getUuid(), deviceId),
            messagesManager.clear(account.getUuid(), deviceId))
            .thenApply(ignored -> account))
        .thenCompose(account -> {
          account.removeDevice(deviceId);

          final List<TransactWriteItem> additionalWriteItems = new ArrayList<>(
              keysManager.buildWriteItemsForRemovedDevice(
                  account.getIdentifier(IdentityType.ACI),
                  account.getIdentifier(IdentityType.PNI),
                  deviceId));

          additionalWriteItems.add(clientPublicKeysManager.buildTransactWriteItemForDeletion(account.getIdentifier(IdentityType.ACI), deviceId));

          return accounts.updateTransactionallyAsync(account, additionalWriteItems)
              .thenApply(ignored -> account);
        })
        .thenCompose(updatedAccount -> redisDeleteAsync(updatedAccount).thenApply(ignored -> updatedAccount))
        // Ensure any messages/single-use pre-keys that came in while we were working are also removed
        .thenCompose(account ->  CompletableFuture.allOf(
                keysManager.deleteSingleUsePreKeys(account.getUuid(), deviceId),
                messagesManager.clear(account.getUuid(), deviceId))
            .thenApply(ignored -> account))
        .exceptionallyCompose(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof ContestedOptimisticLockException && retries > 0) {
            return removeDevice(accountIdentifier, deviceId, retries - 1);
          }

          return CompletableFuture.failedFuture(throwable);
        })
        .whenComplete((ignored, throwable) -> {
          if (throwable == null) {
            disconnectionRequestManager.requestDisconnection(accountIdentifier, List.of(deviceId));
          }
        });
  }

  public Account changeNumber(final Account account,
      final String targetNumber,
      final IdentityKey pniIdentityKey,
      final Map<Byte, ECSignedPreKey> pniSignedPreKeys,
      final Map<Byte, KEMSignedPreKey> pniPqLastResortPreKeys,
      final Map<Byte, Integer> pniRegistrationIds) throws InterruptedException, MismatchedDevicesException {

    final UUID targetPhoneNumberIdentifier = phoneNumberIdentifiers.getPhoneNumberIdentifier(targetNumber).join();

    try {
      return accountLockManager.withLock(new HashSet<>(List.of(account.getPhoneNumberIdentifier(), targetPhoneNumberIdentifier)),
          () -> changeNumber(account, targetNumber, targetPhoneNumberIdentifier, pniIdentityKey, pniSignedPreKeys, pniPqLastResortPreKeys, pniRegistrationIds), accountLockExecutor);
    } catch (final Exception e) {
      if (e instanceof MismatchedDevicesException mismatchedDevicesException) {
        throw mismatchedDevicesException;
      } if (e instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }

      logger.error("Unexpected exception when changing phone number", e);
      throw new RuntimeException(e);
    }
  }

  private Account changeNumber(final Account account,
      final String targetNumber,
      final UUID targetPhoneNumberIdentifier,
      final IdentityKey pniIdentityKey,
      final Map<Byte, ECSignedPreKey> pniSignedPreKeys,
      final Map<Byte, KEMSignedPreKey> pniPqLastResortPreKeys,
      final Map<Byte, Integer> pniRegistrationIds) throws MismatchedDevicesException {

    validateDevices(account, pniSignedPreKeys, pniPqLastResortPreKeys, pniRegistrationIds);

    final UUID originalPhoneNumberIdentifier = account.getPhoneNumberIdentifier();

    redisDelete(account);

    // There are four possible states for accounts associated with the target phone number:
    //
    // 1. The authenticated account already has the given phone number. We don't want to delete the account, but do want
    //    to update keys.
    // 2. An account exists with the target PNI; the caller has proved ownership of the number, so delete the
    //    account with the target PNI. This will leave a "deleted account" record for the deleted account mapping
    //    the UUID of the deleted account to the target PNI. We'll then overwrite that so it points to the
    //    original PNI to facilitate switching back and forth between numbers.
    // 3. No account with the target PNI exists, but one has recently been deleted. In that case, add a "deleted
    //    account" record that maps the ACI of the recently-deleted account to the now-abandoned original PNI
    //    of the account changing its number (which facilitates ACI consistency in cases that a party is switching
    //    back and forth between numbers).
    // 4. No account with the target PNI exists at all, in which case no additional action is needed.
    final Optional<UUID> recentlyDeletedAci = accounts.findRecentlyDeletedAccountIdentifier(targetPhoneNumberIdentifier);
    final Optional<Account> maybeExistingAccount = getByE164(targetNumber);
    final Optional<UUID> maybeDisplacedUuid;

    if (maybeExistingAccount.isPresent()) {
      if (maybeExistingAccount.get().getIdentifier(IdentityType.ACI).equals(account.getIdentifier(IdentityType.ACI))) {
        maybeDisplacedUuid = Optional.empty();
      } else {
        delete(maybeExistingAccount.get()).join();
        maybeDisplacedUuid = maybeExistingAccount.map(Account::getUuid);
      }
    } else {
      maybeDisplacedUuid = recentlyDeletedAci;
    }

    final UUID uuid = account.getUuid();

    CompletableFuture.allOf(
            keysManager.deleteSingleUsePreKeys(targetPhoneNumberIdentifier),
            keysManager.deleteSingleUsePreKeys(originalPhoneNumberIdentifier))
        .join();

      final Collection<TransactWriteItem> keyWriteItems =
          buildPniKeyWriteItems(targetPhoneNumberIdentifier, pniSignedPreKeys, pniPqLastResortPreKeys);

    return updateWithRetries(
        account,
        a -> {
          setPniKeys(account, pniIdentityKey, pniRegistrationIds);
          return true;
        },
        a -> accounts.changeNumber(a, targetNumber, targetPhoneNumberIdentifier, maybeDisplacedUuid, keyWriteItems),
        () -> accounts.getByAccountIdentifier(uuid).orElseThrow(),
        AccountChangeValidator.NUMBER_CHANGE_VALIDATOR);
  }

  private Collection<TransactWriteItem> buildPniKeyWriteItems(
      final UUID phoneNumberIdentifier,
      final Map<Byte, ECSignedPreKey> pniSignedPreKeys,
      final Map<Byte, KEMSignedPreKey> pniPqLastResortPreKeys) {

    final List<TransactWriteItem> keyWriteItems = new ArrayList<>();

    pniSignedPreKeys.forEach((deviceId, signedPreKey) ->
        keyWriteItems.add(keysManager.buildWriteItemForEcSignedPreKey(phoneNumberIdentifier, deviceId, signedPreKey)));

    pniPqLastResortPreKeys.forEach((deviceId, lastResortKey) ->
        keyWriteItems.add(keysManager.buildWriteItemForLastResortKey(phoneNumberIdentifier, deviceId, lastResortKey)));

    return keyWriteItems;
  }

  private void setPniKeys(final Account account,
      final IdentityKey pniIdentityKey,
      final Map<Byte, Integer> pniRegistrationIds) {

    account.getDevices()
        .forEach(device -> device.setPhoneNumberIdentityRegistrationId(pniRegistrationIds.get(device.getId())));

    account.setPhoneNumberIdentityKey(pniIdentityKey);
  }

  private void validateDevices(final Account account,
      final Map<Byte, ECSignedPreKey> pniSignedPreKeys,
      final Map<Byte, KEMSignedPreKey> pniPqLastResortPreKeys,
      final Map<Byte, Integer> pniRegistrationIds) throws MismatchedDevicesException {

    // Check that all including primary ID are in signed pre-keys
    validateCompleteDeviceList(account, pniSignedPreKeys.keySet());

    // Check that all including primary ID are in Pq pre-keys
    validateCompleteDeviceList(account, pniPqLastResortPreKeys.keySet());

    // Check that all devices are accounted for in the map of new PNI registration IDs
    validateCompleteDeviceList(account, pniRegistrationIds.keySet());
  }

  @VisibleForTesting
  static void validateCompleteDeviceList(final Account account, final Set<Byte> deviceIds) throws MismatchedDevicesException {
    final Set<Byte> accountDeviceIds = account.getDevices().stream()
        .map(Device::getId)
        .collect(Collectors.toSet());

    final Set<Byte> missingDeviceIds = new HashSet<>(accountDeviceIds);
    missingDeviceIds.removeAll(deviceIds);

    final Set<Byte> extraDeviceIds = new HashSet<>(deviceIds);
    extraDeviceIds.removeAll(accountDeviceIds);

    if (!missingDeviceIds.isEmpty() || !extraDeviceIds.isEmpty()) {
      throw new MismatchedDevicesException(new MismatchedDevices(missingDeviceIds, extraDeviceIds, Set.of()));
    }
  }

  public record UsernameReservation(Account account, byte[] reservedUsernameHash){}

  /**
   * Reserve a username hash so that no other accounts may take it.
   * <p>
   * The reserved hash can later be set with {@link #confirmReservedUsernameHash(Account, byte[], byte[])}. The reservation
   * will eventually expire, after which point confirmReservedUsernameHash may fail if another account has taken the
   * username hash.
   *
   * @param account the account to update
   * @param requestedUsernameHashes the list of username hashes to attempt to reserve
   * @return a future that yields the reserved username hash and an updated Account object on success; may fail with a
   * {@link UsernameHashNotAvailableException} if none of the given username hashes are available
   */
  public CompletableFuture<UsernameReservation> reserveUsernameHash(final Account account, final List<byte[]> requestedUsernameHashes) {
    if (account.getUsernameHash().filter(
            oldHash -> requestedUsernameHashes.stream().anyMatch(hash -> Arrays.equals(oldHash, hash)))
        .isPresent()) {
      // if we are trying to reserve our already-confirmed username hash, we don't need to do
      // anything, and can give the client a success response (they may try to confirm it again,
      // but that's a no-op other than rotaing their username link which they may need to do
      // anyway). note this is *not* the case for reserving our already-reserved username hash,
      // which should extend the reservation's TTL.
      return CompletableFuture.completedFuture(new UsernameReservation(account, account.getUsernameHash().get()));
    }

    final AtomicReference<byte[]> reservedUsernameHash = new AtomicReference<>();

    return redisDeleteAsync(account)
        .thenCompose(ignored -> updateWithRetriesAsync(
            account,
            a -> true,
            a -> checkAndReserveNextUsernameHash(a, new ArrayDeque<>(requestedUsernameHashes))
                .thenAccept(reservedUsernameHash::set),
            () -> accounts.getByAccountIdentifierAsync(account.getUuid()).thenApply(Optional::orElseThrow),
            AccountChangeValidator.USERNAME_CHANGE_VALIDATOR,
            MAX_UPDATE_ATTEMPTS))
        .whenComplete((updatedAccount, throwable) -> {
          if (throwable == null) {
            // Make a best effort to clear any stale data that may have been cached while this operation was in progress
            redisDeleteAsync(updatedAccount);
          }
        })
        .thenApply(updatedAccount -> new UsernameReservation(updatedAccount, reservedUsernameHash.get()));
  }

  private CompletableFuture<byte[]> checkAndReserveNextUsernameHash(final Account account, final Queue<byte[]> requestedUsernameHashes) {
    final byte[] usernameHash = requestedUsernameHashes.remove();

    return accounts.reserveUsernameHash(account, usernameHash, USERNAME_HASH_RESERVATION_TTL_MINUTES)
        .thenApply(ignored -> usernameHash)
        .exceptionallyComposeAsync(
            throwable -> {
              if (ExceptionUtils.unwrap(throwable) instanceof UsernameHashNotAvailableException && !requestedUsernameHashes.isEmpty()) {
                return checkAndReserveNextUsernameHash(account, requestedUsernameHashes);
              }
              return CompletableFuture.failedFuture(throwable);
            });
  }

  /**
   * Set a username hash previously reserved with {@link #reserveUsernameHash(Account, List)}
   *
   * @param account the account to update
   * @param reservedUsernameHash the previously reserved username hash
   * @param encryptedUsername the encrypted form of the previously reserved username for the username link
   * @return a future that yields the updated account with the username hash field set; may fail with a
   * {@link UsernameHashNotAvailableException} if the reserved username hash has been taken (because the reservation
   * expired) or a {@link UsernameReservationNotFoundException} if {@code reservedUsernameHash} was not reserved in the
   * account
   */
  public CompletableFuture<Account> confirmReservedUsernameHash(final Account account, final byte[] reservedUsernameHash, @Nullable final byte[] encryptedUsername) {
    if (account.getUsernameHash().map(currentUsernameHash -> Arrays.equals(currentUsernameHash, reservedUsernameHash)).orElse(false)) {
      // the client likely already succeeded and is retrying
      return CompletableFuture.completedFuture(account);
    }

    if (!account.getReservedUsernameHash().map(oldHash -> Arrays.equals(oldHash, reservedUsernameHash)).orElse(false)) {
      // no such reservation existed, either there was no previous call to reserveUsername
      // or the reservation changed
      return CompletableFuture.failedFuture(new UsernameReservationNotFoundException());
    }

    return redisDeleteAsync(account)
        .thenCompose(ignored -> updateWithRetriesAsync(
            account,
            a -> true,
            a -> accounts.confirmUsernameHash(a, reservedUsernameHash, encryptedUsername),
            () -> accounts.getByAccountIdentifierAsync(account.getUuid()).thenApply(Optional::orElseThrow),
            AccountChangeValidator.USERNAME_CHANGE_VALIDATOR,
            MAX_UPDATE_ATTEMPTS
        ))
        .whenComplete((updatedAccount, throwable) -> {
          if (throwable == null) {
            // Make a best effort to clear any stale data that may have been cached while this operation was in progress
            redisDeleteAsync(updatedAccount);
          }
        });
  }

  public CompletableFuture<Account> clearUsernameHash(final Account account) {
    return redisDeleteAsync(account)
        .thenCompose(ignored -> updateWithRetriesAsync(
            account,
            a -> true,
            accounts::clearUsernameHash,
            () -> accounts.getByAccountIdentifierAsync(account.getUuid()).thenApply(Optional::orElseThrow),
            AccountChangeValidator.USERNAME_CHANGE_VALIDATOR,
            MAX_UPDATE_ATTEMPTS))
        .whenComplete((updatedAccount, throwable) -> {
          if (throwable == null) {
            // Make a best effort to clear any stale data that may have been cached while this operation was in progress
            redisDeleteAsync(updatedAccount);
          }
        });
  }

  public Account update(Account account, Consumer<Account> updater) {
    return update(account, a -> {
      updater.accept(a);
      // assume that all updaters passed to the public method actually modify the account
      return true;
    });
  }

  public CompletableFuture<Account> updateAsync(Account account, Consumer<Account> updater) {
    return updateAsync(account, a -> {
      updater.accept(a);
      // assume that all updaters passed to the public method actually modify the account
      return true;
    });
  }

  /**
   * Specialized version of {@link #updateDevice(Account, byte, Consumer)} that minimizes potentially contentious and
   * redundant updates of {@code device.lastSeen}
   */
  public Account updateDeviceLastSeen(Account account, Device device, final long lastSeen) {
    return update(account, a -> {

      final Optional<Device> maybeDevice = a.getDevice(device.getId());

      return maybeDevice.map(d -> {
        if (d.getLastSeen() >= lastSeen) {
          return false;
        }

        d.setLastSeen(lastSeen);

        return true;

      }).orElse(false);
    });
  }

  public Account updateDeviceAuthentication(final Account account, final Device device, final SaltedTokenHash credentials) {
    Preconditions.checkArgument(credentials.getVersion() == SaltedTokenHash.CURRENT_VERSION);
    return updateDevice(account, device.getId(), device1 -> device1.setAuthTokenHash(credentials));
  }

  /**
   * @param account account to update
   * @param updater must return {@code true} if the account was actually updated
   */
  private Account update(Account account, Function<Account, Boolean> updater) {

    return updateTimer.record(() -> {

      redisDelete(account);

      final UUID uuid = account.getUuid();

      final Account updatedAccount = updateWithRetries(account,
          updater,
          accounts::update,
          () -> accounts.getByAccountIdentifier(uuid).orElseThrow(),
          AccountChangeValidator.GENERAL_CHANGE_VALIDATOR);

      redisSet(updatedAccount);

      return updatedAccount;
    });
  }

  private CompletableFuture<Account> updateAsync(final Account account, final Function<Account, Boolean> updater) {

    final Timer.Sample timerSample = Timer.start();

    return redisDeleteAsync(account)
        .thenCompose(ignored -> {
          final UUID uuid = account.getUuid();

          return updateWithRetriesAsync(account,
              updater,
              a -> accounts.updateAsync(a).toCompletableFuture(),
              () -> accounts.getByAccountIdentifierAsync(uuid).thenApply(Optional::orElseThrow),
              AccountChangeValidator.GENERAL_CHANGE_VALIDATOR,
              MAX_UPDATE_ATTEMPTS);
        })
        .thenCompose(updatedAccount -> redisSetAsync(updatedAccount).thenApply(ignored -> updatedAccount))
        .whenComplete((ignored, throwable) -> timerSample.stop(updateTimer));
  }

  private Account updateWithRetries(Account account,
      final Function<Account, Boolean> updater,
      final Consumer<Account> persister,
      final Supplier<Account> retriever,
      final AccountChangeValidator changeValidator) {

    Account originalAccount = AccountUtil.cloneAccountAsNotStale(account);

    if (!updater.apply(account)) {
      return account;
    }

    final int maxTries = 10;
    int tries = 0;

    while (tries < maxTries) {

      try {
        persister.accept(account);

        final Account updatedAccount = AccountUtil.cloneAccountAsNotStale(account);
        account.markStale();

        changeValidator.validateChange(originalAccount, updatedAccount);

        return updatedAccount;
      } catch (final ContestedOptimisticLockException e) {
        tries++;

        account = retriever.get();
        originalAccount = AccountUtil.cloneAccountAsNotStale(account);

        if (!updater.apply(account)) {
          return account;
        }
      }
    }

    throw new OptimisticLockRetryLimitExceededException();
  }

  private CompletionStage<Account> updateWithRetriesAsync(Account account,
      final Function<Account, Boolean> updater,
      final Function<Account, CompletionStage<Void>> persister,
      final Supplier<CompletionStage<Account>> retriever,
      final AccountChangeValidator changeValidator,
      final int remainingTries) {

    final Account originalAccount = AccountUtil.cloneAccountAsNotStale(account);

    if (!updater.apply(account)) {
      return CompletableFuture.completedFuture(account);
    }

    if (remainingTries > 0) {
      return persister.apply(account)
          .thenApply(ignored -> {
            final Account updatedAccount = AccountUtil.cloneAccountAsNotStale(account);
            account.markStale();

            changeValidator.validateChange(originalAccount, updatedAccount);

            return updatedAccount;
          })
          .exceptionallyCompose(throwable -> {
            if (ExceptionUtils.unwrap(throwable) instanceof ContestedOptimisticLockException) {
              return retriever.get().thenCompose(refreshedAccount ->
                  updateWithRetriesAsync(refreshedAccount, updater, persister, retriever, changeValidator, remainingTries - 1));
            } else {
              throw ExceptionUtils.wrap(throwable);
            }
          });
    }

    return CompletableFuture.failedFuture(new OptimisticLockRetryLimitExceededException());
  }

  public Account updateDevice(Account account, byte deviceId, Consumer<Device> deviceUpdater) {
    return update(account, a -> {
      a.getDevice(deviceId).ifPresent(deviceUpdater);
      // assume that all updaters passed to the public method actually modify the device
      return true;
    });
  }

  public CompletableFuture<Account> updateDeviceAsync(final Account account, final byte deviceId,
      final Consumer<Device> deviceUpdater) {
    return updateAsync(account, a -> {
      a.getDevice(deviceId).ifPresent(deviceUpdater);
      // assume that all updaters passed to the public method actually modify the device
      return true;
    });
  }

  public Optional<Account> getByE164(final String number) {
    return getByNumberTimer.record(() -> accounts.getByE164(number));
  }

  public CompletableFuture<Optional<Account>> getByE164Async(final String number) {
    Timer.Sample sample = Timer.start();
    return accounts.getByE164Async(number)
        .whenComplete((ignoredResult, ignoredThrowable) -> sample.stop(getByNumberTimer));
  }

  public Optional<Account> getByPhoneNumberIdentifier(final UUID pni) {
    return checkRedisThenAccounts(
        getByNumberTimer,
        () -> redisGetBySecondaryKey(getAccountMapKey(pni.toString()), redisPniGetTimer),
        () -> accounts.getByPhoneNumberIdentifier(pni)
    );
  }

  public CompletableFuture<Optional<Account>> getByPhoneNumberIdentifierAsync(final UUID pni) {
    return checkRedisThenAccountsAsync(
        getByNumberTimer,
        () -> redisGetBySecondaryKeyAsync(getAccountMapKey(pni.toString()), redisPniGetTimer),
        () -> accounts.getByPhoneNumberIdentifierAsync(pni)
    );
  }

  public CompletableFuture<Optional<Account>> getByUsernameLinkHandle(final UUID usernameLinkHandle) {
    final Timer.Sample sample = Timer.start();
    return accounts.getByUsernameLinkHandle(usernameLinkHandle)
        .whenComplete((ignoredResult, ignoredThrowable) -> sample.stop(getByUsernameLinkHandleTimer));
  }

  public CompletableFuture<Optional<Account>> getByUsernameHash(final byte[] usernameHash) {
    final Timer.Sample sample = Timer.start();
    return accounts.getByUsernameHash(usernameHash)
        .whenComplete((ignoredResult, ignoredThrowable) -> sample.stop(getByUsernameHashTimer));
  }

  public Optional<Account> getByServiceIdentifier(final ServiceIdentifier serviceIdentifier) {
    return switch (serviceIdentifier.identityType()) {
      case ACI -> getByAccountIdentifier(serviceIdentifier.uuid());
      case PNI -> getByPhoneNumberIdentifier(serviceIdentifier.uuid());
    };
  }

  public CompletableFuture<Optional<Account>> getByServiceIdentifierAsync(final ServiceIdentifier serviceIdentifier) {
    return switch (serviceIdentifier.identityType()) {
      case ACI -> getByAccountIdentifierAsync(serviceIdentifier.uuid());
      case PNI -> getByPhoneNumberIdentifierAsync(serviceIdentifier.uuid());
    };
  }

  public Optional<Account> getByAccountIdentifier(final UUID uuid) {
    return checkRedisThenAccounts(
        getByUuidTimer,
        () -> redisGetByAccountIdentifier(uuid),
        () -> accounts.getByAccountIdentifier(uuid)
    );
  }

  public CompletableFuture<Optional<Account>> getByAccountIdentifierAsync(final UUID uuid) {
    return checkRedisThenAccountsAsync(
        getByUuidTimer,
        () -> redisGetByAccountIdentifierAsync(uuid),
        () -> accounts.getByAccountIdentifierAsync(uuid)
    );
  }

  public UUID getPhoneNumberIdentifier(String e164) {
    return phoneNumberIdentifiers.getPhoneNumberIdentifier(e164).join();
  }

  public Optional<UUID> findRecentlyDeletedAccountIdentifier(final UUID phoneNumberIdentifier) {
    return accounts.findRecentlyDeletedAccountIdentifier(phoneNumberIdentifier);
  }

  public Optional<UUID> findRecentlyDeletedPhoneNumberIdentifier(final UUID accountIdentifier) {
    return accounts.findRecentlyDeletedPhoneNumberIdentifier(accountIdentifier);
  }

  public Flux<Account> streamAllFromDynamo(final int segments, final Scheduler scheduler) {
    return accounts.getAll(segments, scheduler);
  }

  public Flux<UUID> streamAccountIdentifiersFromDynamo(final int segments, final Scheduler scheduler) {
    return accounts.getAllAccountIdentifiers(segments, scheduler);
  }

  public CompletableFuture<Void> delete(final Account account, final DeletionReason deletionReason) {
    final Timer.Sample sample = Timer.start();

    return accountLockManager.withLockAsync(Set.of(account.getPhoneNumberIdentifier()), () -> delete(account),
            accountLockExecutor)
        .whenComplete((ignored, throwable) -> {
          sample.stop(deleteTimer);

          if (throwable == null) {
            Metrics.counter(DELETE_COUNTER_NAME,
                    COUNTRY_CODE_TAG_NAME, Util.getCountryCode(account.getNumber()),
                    DELETION_REASON_TAG_NAME, deletionReason.tagValue)
                .increment();
          } else {
            logger.warn("Failed to delete account", throwable);
          }
        });
  }

  private CompletableFuture<Void> delete(final Account account) {
    final List<TransactWriteItem> additionalWriteItems = Stream.concat(
            account.getDevices().stream()
                .flatMap(device -> keysManager.buildWriteItemsForRemovedDevice(
                        account.getIdentifier(IdentityType.ACI),
                        account.getIdentifier(IdentityType.PNI),
                        device.getId())
                    .stream()),
            account.getDevices().stream()
                .map(device -> clientPublicKeysManager.buildTransactWriteItemForDeletion(
                    account.getIdentifier(IdentityType.ACI),
                    device.getId())))
        .toList();
    return CompletableFuture.allOf(
            secureStorageClient.deleteStoredData(account.getUuid()),
            secureValueRecovery2Client.removeData(account.getUuid()),
            keysManager.deleteSingleUsePreKeys(account.getUuid()),
            keysManager.deleteSingleUsePreKeys(account.getPhoneNumberIdentifier()),
            messagesManager.clear(account.getUuid()),
            profilesManager.deleteAll(account.getUuid(), true),
            registrationRecoveryPasswordsManager.remove(account.getIdentifier(IdentityType.PNI)))
        .thenCompose(ignored -> accounts.delete(account.getUuid(), additionalWriteItems))
        .thenCompose(ignored -> redisDeleteAsync(account))
        .thenRun(() -> disconnectionRequestManager.requestDisconnection(account));
  }

  private String getAccountMapKey(String key) {
    return "AccountMap::" + key;
  }

  private String getAccountEntityKey(UUID uuid) {
    return "Account3::" + uuid.toString();
  }

  private void redisSet(Account account) {
    redisSetTimer.record(() -> {
      try {
        final String accountJson = writeRedisAccountJson(account);

        cacheCluster.useCluster(connection -> {
          final RedisAdvancedClusterCommands<String, String> commands = connection.sync();

          commands.setex(getAccountMapKey(account.getPhoneNumberIdentifier().toString()), CACHE_TTL_SECONDS,
              account.getUuid().toString());
          commands.setex(getAccountEntityKey(account.getUuid()), CACHE_TTL_SECONDS, accountJson);
        });
      } catch (JsonProcessingException e) {
        throw new IllegalStateException(e);
      }
    });
  }

  private CompletableFuture<Void> redisSetAsync(final Account account) {
    final String accountJson;

    try {
      accountJson = writeRedisAccountJson(account);
    } catch (final JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }

    return cacheCluster.withCluster(connection -> CompletableFuture.allOf(
        connection.async().setex(
                getAccountMapKey(account.getPhoneNumberIdentifier().toString()), CACHE_TTL_SECONDS,
                account.getUuid().toString())
            .toCompletableFuture(),
        connection.async().setex(getAccountEntityKey(account.getUuid()), CACHE_TTL_SECONDS, accountJson)
            .toCompletableFuture()));
  }

  private Optional<Account> checkRedisThenAccounts(
      final Timer overallTimer,
      final Supplier<Optional<Account>> resolveFromRedis,
      final Supplier<Optional<Account>> resolveFromAccounts) {
    return overallTimer.record(() -> {
      Optional<Account> account = resolveFromRedis.get();
      if (account.isEmpty()) {
        account = resolveFromAccounts.get();
        account.ifPresent(this::redisSet);
      }
      return account;
    });
  }

  private CompletableFuture<Optional<Account>> checkRedisThenAccountsAsync(
      final Timer overallTimer,
      final Supplier<CompletableFuture<Optional<Account>>> resolveFromRedis,
      final Supplier<CompletableFuture<Optional<Account>>> resolveFromAccounts) {

    final Timer.Sample sample = Timer.start();

    return resolveFromRedis.get()
        .thenCompose(maybeAccountFromRedis -> maybeAccountFromRedis
            .map(accountFromRedis -> CompletableFuture.completedFuture(maybeAccountFromRedis))
            .orElseGet(() -> resolveFromAccounts.get()
                .thenCompose(maybeAccountFromAccounts -> maybeAccountFromAccounts
                    .map(account -> redisSetAsync(account).thenApply(ignored -> maybeAccountFromAccounts))
                    .orElseGet(() -> CompletableFuture.completedFuture(maybeAccountFromAccounts)))))
        .whenComplete((ignored, throwable) -> sample.stop(overallTimer));
  }

  private Optional<Account> redisGetBySecondaryKey(final String secondaryKey, final Timer timer) {
    return timer.record(() -> {
      try {
      return Optional.ofNullable(cacheCluster.withCluster(connection -> connection.sync().get(secondaryKey)))
          .map(UUID::fromString)
          .flatMap(this::getByAccountIdentifier);
    } catch (IllegalArgumentException e) {
      logger.warn("Deserialization error", e);
      return Optional.empty();
    } catch (RedisException e) {
      logger.warn("Redis failure", e);
      return Optional.empty();
    }
    });
  }

  private CompletableFuture<Optional<Account>> redisGetBySecondaryKeyAsync(final String secondaryKey, final Timer timer) {
    final Timer.Sample sample = Timer.start();

    return cacheCluster.withCluster(connection -> connection.async().get(secondaryKey))
        .thenCompose(nullableUuid -> {
          if (nullableUuid != null) {
            return getByAccountIdentifierAsync(UUID.fromString(nullableUuid));
          } else {
            return CompletableFuture.completedFuture(Optional.empty());
          }
        })
        .exceptionally(throwable -> {
          logger.warn("Failed to retrieve account from Redis", throwable);
          return Optional.empty();
        })
        .whenComplete((ignored, throwable) -> sample.stop(timer))
        .toCompletableFuture();
  }

  private Optional<Account> redisGetByAccountIdentifier(UUID uuid) {
    return redisUuidGetTimer.record(() -> {
      try {
        final String json = cacheCluster.withCluster(connection -> connection.sync().get(getAccountEntityKey(uuid)));

        return parseAccountJson(json, uuid);
      } catch (final RedisException e) {
        logger.warn("Redis failure", e);
        return Optional.empty();
      }
    });
  }

  private CompletableFuture<Optional<Account>> redisGetByAccountIdentifierAsync(final UUID uuid) {
    return cacheCluster.withCluster(connection -> connection.async().get(getAccountEntityKey(uuid)))
        .thenApply(accountJson -> parseAccountJson(accountJson, uuid))
        .exceptionally(throwable -> {
          logger.warn("Failed to retrieve account from Redis", throwable);
          return Optional.empty();
        })
        .toCompletableFuture();
  }

  @VisibleForTesting
  static Optional<Account> parseAccountJson(@Nullable final String accountJson, final UUID uuid) {
    try {
      if (StringUtils.isNotBlank(accountJson)) {
        Account account = SystemMapper.jsonMapper().readValue(accountJson, Account.class);
        account.setUuid(uuid);

        if (account.getPhoneNumberIdentifier() == null) {
          logger.warn("Account {} loaded from Redis is missing a PNI", uuid);
        }

        return Optional.of(account);
      }

      return Optional.empty();
    } catch (final IOException e) {
      logger.warn("Deserialization error", e);
      return Optional.empty();
    }
  }

  @VisibleForTesting
  static String writeRedisAccountJson(final Account account) throws JsonProcessingException {
    return ACCOUNT_REDIS_JSON_WRITER.writeValueAsString(account);
  }

  private void redisDelete(final Account account) {
    ResilienceUtil.getGeneralRedisRetry(RETRY_NAME).executeRunnable(() ->
        redisDeleteTimer.record(() ->
            cacheCluster.useCluster(connection ->
                connection.sync().del(getAccountMapKey(account.getPhoneNumberIdentifier().toString()),
                    getAccountEntityKey(account.getUuid())))));
  }

  private CompletableFuture<Void> redisDeleteAsync(final Account account) {
    final Timer.Sample sample = Timer.start();

    final String[] keysToDelete = new String[]{
        getAccountMapKey(account.getPhoneNumberIdentifier().toString()),
        getAccountEntityKey(account.getUuid())
    };

    return ResilienceUtil.getGeneralRedisRetry(RETRY_NAME).executeCompletionStage(retryExecutor,
            () -> cacheCluster.withCluster(connection -> connection.async().del(keysToDelete))
                .thenRun(Util.NOOP))
        .toCompletableFuture()
        .whenComplete((_, _) -> sample.stop(redisDeleteTimer));
  }

  public CompletableFuture<Optional<DeviceInfo>> waitForNewLinkedDevice(
      final UUID accountIdentifier,
      final Device linkingDevice,
      final String linkDeviceTokenIdentifier,
      final Duration timeout) {
    if (!linkingDevice.isPrimary()) {
      throw new IllegalArgumentException("Only primary devices can link devices");
    }

    // Unbeknownst to callers but beknownst to us, the "link device token identifier" is the base64/url-encoded SHA256
    // hash of a device-linking token. Before we use the string anywhere, make sure it's the right "shape" for a hash.
    if (Base64.getUrlDecoder().decode(linkDeviceTokenIdentifier).length != SHA256_HASH_LENGTH) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid token identifier"));
    }

    final Instant deadline = clock.instant().plus(timeout);
    final CompletableFuture<Optional<DeviceInfo>> deviceAdded = waitForPubSubKey(waitForDeviceFuturesByTokenIdentifier,
        linkDeviceTokenIdentifier, getLinkedDeviceKey(linkDeviceTokenIdentifier), timeout, this::handleDeviceAdded);

    return deviceAdded.thenCompose(maybeDeviceInfo -> maybeDeviceInfo.map(deviceInfo -> {
          // The device finished linking, we now want to make sure the client has fetched messages that could
          // have come in before the device's mailbox was set up.

          // A worst case estimate of the wall clock time at which the linked device was added to the account record
          Instant deviceLinked = Instant.ofEpochMilli(deviceInfo.created()).plus(MAX_SERVER_CLOCK_DRIFT);

          Instant now = clock.instant();

          // We know at `now` the device finished linking, so if we waited for all the messages before now it would be
          // sufficient. However, now might be much later that the device was linked, so we don't want to force
          // the client to wait for messages that are past our worst case estimate of when the device was linked
          Instant messageEpoch = Collections.min(List.of(deviceLinked, now));

          // We assume that any message with a timestamp after the messageEpoch made it into the linked device's queues
          return waitForPreLinkMessagesToBeFetched(accountIdentifier, linkingDevice, deviceInfo, messageEpoch, deadline);
        })
        .orElseGet(() -> CompletableFuture.completedFuture(maybeDeviceInfo)));
  }

  /**
   * Wait until there are no pending messages for the authenticatedDevice that have a timestamp lower than the provided
   * messageEpoch.
   *
   * @param aci              The account identifier of the device doing the linking
   * @param linkingDevice    The device doing the linking
   * @param linkedDeviceInfo Information about the newly linked device
   * @param messageEpoch     A time at which the device was linked
   * @param deadline         The time at which the method will stop waiting
   * @return A future that completes when there are no pending messages for the linking device with a timestamp earlier
   * the provided messageEpoch, or after the deadline is reached. If the deadline was exceeded, the future will be empty.
   */
  private CompletableFuture<Optional<DeviceInfo>> waitForPreLinkMessagesToBeFetched(
      final UUID aci,
      final Device linkingDevice,
      final DeviceInfo linkedDeviceInfo,
      final Instant messageEpoch,
      final Instant deadline) {
    return messagesManager.getEarliestUndeliveredTimestampForDevice(aci, linkingDevice)
        .thenCompose(maybeEarliestTimestamp -> {

          final boolean clientHasOldMessages = maybeEarliestTimestamp
              .map(earliestTimestamp -> earliestTimestamp.isBefore(messageEpoch))
              .orElse(false);

          if (!clientHasOldMessages) {
            // The client has fetched all messages before the messageEpoch
            return CompletableFuture.completedFuture(Optional.of(linkedDeviceInfo));
          }

          final Instant now = clock.instant();
          if (now.plus(MESSAGE_POLL_INTERVAL).isAfter(deadline)) {
            // Not enough time to try again before the deadline
            return CompletableFuture.completedFuture(Optional.empty());
          }

          // Schedule a retry
          return CompletableFuture.supplyAsync(
                  () -> waitForPreLinkMessagesToBeFetched(aci, linkingDevice, linkedDeviceInfo, messageEpoch, deadline),
                  r -> messagesPollExecutor.schedule(r, MESSAGE_POLL_INTERVAL.toMillis(), TimeUnit.MILLISECONDS))
              .thenCompose(Function.identity());
        });
  }


  private void handleDeviceAdded(final CompletableFuture<Optional<DeviceInfo>> future, final String deviceInfoJson) {
    try {
      future.complete(Optional.of(SystemMapper.jsonMapper().readValue(deviceInfoJson, DeviceInfo.class)));
    } catch (final JsonProcessingException e) {
      logger.error("Could not parse device json", e);
      future.completeExceptionally(e);
    }
  }

  private static String getLinkedDeviceKey(final String linkDeviceTokenIdentifier) {
    return LINKED_DEVICE_PREFIX + linkDeviceTokenIdentifier;
  }

  public CompletableFuture<Optional<TransferArchiveResult>> waitForTransferArchive(final Account account, final Device device, final Duration timeout) {
    final DeviceIdentifier timestampDeviceIdentifier = new TimestampDeviceIdentifier(account.getIdentifier(IdentityType.ACI), device.getId(), Instant.ofEpochMilli(device.getCreated()));
    final String timestampTransferArchiveKey = getTimestampTransferArchiveKey(account.getIdentifier(IdentityType.ACI), device.getId(), Instant.ofEpochMilli(device.getCreated()));

    final DeviceIdentifier registrationIdDeviceIdentifier = new RegistrationIdDeviceIdentifier(account.getIdentifier(IdentityType.ACI), device.getId(), device.getRegistrationId(IdentityType.ACI));
    final String registrationIdTransferArchiveKey = getRegistrationIdTransferArchiveKey(account.getIdentifier(IdentityType.ACI), device.getId(), device.getRegistrationId(IdentityType.ACI));

    final CompletableFuture<Optional<TransferArchiveResult>> timestampFuture = waitForPubSubKey(waitForTransferArchiveFuturesByDeviceIdentifier,
        timestampDeviceIdentifier,
        timestampTransferArchiveKey,
        timeout,
        this::handleTransferArchiveAdded);

    final CompletableFuture<Optional<TransferArchiveResult>> registrationIdFuture = waitForPubSubKey(waitForTransferArchiveFuturesByDeviceIdentifier,
        registrationIdDeviceIdentifier,
        registrationIdTransferArchiveKey,
        timeout,
        this::handleTransferArchiveAdded);
    return firstSuccessfulTransferArchiveFuture(List.of(timestampFuture, registrationIdFuture));
  }

  @VisibleForTesting
  static CompletableFuture<Optional<TransferArchiveResult>> firstSuccessfulTransferArchiveFuture(
      final List<CompletableFuture<Optional<TransferArchiveResult>>> futures) {
    final CompletableFuture<Optional<TransferArchiveResult>> result = new CompletableFuture<>();
    final AtomicInteger remaining = new AtomicInteger(futures.size());

    for (CompletableFuture<Optional<TransferArchiveResult>> future : futures) {
      future.whenComplete((value, _) -> {
        if (value.isPresent()) {
          result.complete(value);
        } else if (remaining.decrementAndGet() == 0) {
          result.complete(Optional.empty());
        }
      });
    }

    return result;
  }

  public CompletableFuture<Void> recordTransferArchiveUpload(final Account account,
      final byte destinationDeviceId,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<Instant> destinationDeviceCreationTimestamp,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<Integer> maybeRegistrationId,
      final TransferArchiveResult transferArchiveResult) {
    try {
      final String transferArchiveJson = SystemMapper.jsonMapper().writeValueAsString(transferArchiveResult);

      final String key = destinationDeviceCreationTimestamp
          .map(timestamp -> getTimestampTransferArchiveKey(account.getIdentifier(IdentityType.ACI), destinationDeviceId, timestamp))
          .orElseGet(() -> maybeRegistrationId
              .map(registrationId -> getRegistrationIdTransferArchiveKey(account.getIdentifier(IdentityType.ACI), destinationDeviceId, registrationId))
              // We validate the request object so this should never happen
              .orElseThrow(() -> new AssertionError("No creation timestamp or registration ID provided")));

      return ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
          .executeCompletionStage(retryExecutor, () -> pubSubRedisClient.withConnection(connection -> connection.async()
                  .set(key, transferArchiveJson, SetArgs.Builder.ex(RECENTLY_ADDED_TRANSFER_ARCHIVE_TTL)))
              .toCompletableFuture())
          .thenRun(Util.NOOP)
          .toCompletableFuture();
    } catch (final JsonProcessingException e) {
      // This should never happen for well-defined objects we control
      throw new UncheckedIOException(e);
    }
  }

  private void handleTransferArchiveAdded(final CompletableFuture<Optional<TransferArchiveResult>> future, final String transferArchiveJson) {
    try {
      future.complete(Optional.of(SystemMapper.jsonMapper().readValue(transferArchiveJson, TransferArchiveResult.class)));
    } catch (final JsonProcessingException e) {
      logger.error("Could not parse transfer archive json", e);
      future.completeExceptionally(e);
    }
  }

  private static String getTimestampTransferArchiveKey(final UUID accountIdentifier,
      final byte destinationDeviceId,
      final Instant destinationDeviceCreationTimestamp) {
    Metrics.counter(TIMESTAMP_BASED_TRANSFER_ARCHIVE_KEY_COUNTER_NAME).increment();

    return TRANSFER_ARCHIVE_PREFIX + accountIdentifier.toString() +
        ":" + destinationDeviceId +
        ":" + destinationDeviceCreationTimestamp.toEpochMilli();
  }

  private static String getRegistrationIdTransferArchiveKey(final UUID accountIdentifier,
      final byte destinationDeviceId,
      final int registrationId) {
    Metrics.counter(REGISTRATION_ID_BASED_TRANSFER_ARCHIVE_KEY_COUNTER_NAME).increment();

    return TRANSFER_ARCHIVE_PREFIX + accountIdentifier.toString() +
        ":" + destinationDeviceId +
        ":" + TRANSFER_ARCHIVE_REGISTRATION_ID_PATTERN +
        ":" + registrationId;
  }

  public CompletableFuture<Optional<RestoreAccountRequest>> waitForRestoreAccountRequest(final String token, final Duration timeout) {
    return waitForPubSubKey(waitForRestoreAccountRequestFuturesByToken,
        token,
        getRestoreAccountRequestKey(token),
        timeout,
        this::handleRestoreAccountRequest);
  }

  public CompletableFuture<Void> recordRestoreAccountRequest(final String token, final RestoreAccountRequest restoreAccountRequest) {
    final String key = getRestoreAccountRequestKey(token);

    final String requestJson;

    try {
      requestJson = SystemMapper.jsonMapper().writeValueAsString(restoreAccountRequest);
    } catch (final JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }

    return ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
        .executeCompletionStage(retryExecutor, () -> pubSubRedisClient.withConnection(connection ->
                connection.async().set(key, requestJson, SetArgs.Builder.ex(RESTORE_ACCOUNT_REQUEST_TTL)))
            .toCompletableFuture())
        .thenRun(Util.NOOP)
        .toCompletableFuture();
  }

  private void handleRestoreAccountRequest(final CompletableFuture<Optional<RestoreAccountRequest>> future, final String transferRequestJson) {
    try {
      future.complete(Optional.of(SystemMapper.jsonMapper().readValue(transferRequestJson, RestoreAccountRequest.class)));
    } catch (final JsonProcessingException e) {
      logger.error("Could not parse device transfer request JSON", e);
      future.completeExceptionally(e);
    }
  }

  private static String getRestoreAccountRequestKey(final String token) {
    return RESTORE_ACCOUNT_REQUEST_PREFIX + token;
  }

  private <K, T> CompletableFuture<Optional<T>> waitForPubSubKey(final Map<K, CompletableFuture<Optional<T>>> futureMap,
      final K mapKey,
      final String redisKey,
      final Duration timeout,
      final BiConsumer<CompletableFuture<Optional<T>>, String> handler) {

    final CompletableFuture<Optional<T>> future = new CompletableFuture<>();

    future.completeOnTimeout(Optional.empty(), TimeUnit.MILLISECONDS.convert(timeout), TimeUnit.MILLISECONDS)
        .whenComplete((maybeBackup, throwable) -> futureMap.remove(mapKey, future));

    {
      final CompletableFuture<Optional<T>> displacedFuture = futureMap.put(mapKey, future);

      if (displacedFuture != null) {
        displacedFuture.complete(Optional.empty());
      }
    }

    // The Redis key we're waiting for may have been added before the caller issued a request to watch for it; check to
    // see if it's already there
    pubSubRedisClient.withConnection(connection -> connection.async().get(redisKey))
        .thenAccept(response -> {
          if (StringUtils.isNotBlank(response)) {
            handler.accept(future, response);
          }
        });

    return future;
  }

  @Override
  public void message(final String pattern, final String channel, final String message) {
    if (LINKED_DEVICE_KEYSPACE_PATTERN.equals(pattern) && "set".equalsIgnoreCase(message)) {
      // The `- 1` here compensates for the '*' in the pattern
      final String tokenIdentifier = channel.substring(LINKED_DEVICE_KEYSPACE_PATTERN.length() - 1);

      Optional.ofNullable(waitForDeviceFuturesByTokenIdentifier.remove(tokenIdentifier))
          .ifPresent(future -> pubSubRedisClient.withConnection(connection -> connection.async().get(getLinkedDeviceKey(tokenIdentifier)))
              .whenComplete((deviceInfoJson, throwable) -> {
                if (throwable != null) {
                  future.completeExceptionally(throwable);
                } else {
                  handleDeviceAdded(future, deviceInfoJson);
                }
              }));
    } else if (TRANSFER_ARCHIVE_KEYSPACE_PATTERN.equals(pattern) && "set".equalsIgnoreCase(message)) {
      // The `- 1` here compensates for the '*' in the pattern
      final String[] deviceIdentifierComponents =
          channel.substring(TRANSFER_ARCHIVE_KEYSPACE_PATTERN.length() - 1).split(":", 4);

      if (deviceIdentifierComponents.length != 3 && deviceIdentifierComponents.length != 4) {
        logger.error("Could not parse device identifier; unexpected component count");
        return;
      }

      final DeviceIdentifier deviceIdentifier;
      final String transferArchiveKey;
      try {
        final UUID accountIdentifier = UUID.fromString(deviceIdentifierComponents[0]);
        final byte deviceId = Byte.parseByte(deviceIdentifierComponents[1]);

        if (deviceIdentifierComponents.length == 3) {
          // Parse the old transfer archive Redis key format
          final Instant deviceCreationTimestamp = Instant.ofEpochMilli(Long.parseLong(deviceIdentifierComponents[2]));

          deviceIdentifier = new TimestampDeviceIdentifier(accountIdentifier, deviceId, deviceCreationTimestamp);
          transferArchiveKey = getTimestampTransferArchiveKey(accountIdentifier, deviceId, deviceCreationTimestamp);
        } else {
          final String maybeRegistrationIdPattern = deviceIdentifierComponents[2];
          if (!maybeRegistrationIdPattern.equals(TRANSFER_ARCHIVE_REGISTRATION_ID_PATTERN)) {
            throw new IllegalArgumentException("Could not parse Redis key with pattern " + maybeRegistrationIdPattern);
          }
          final int registrationId = Integer.parseInt(deviceIdentifierComponents[3]);
          if (!RegistrationIdValidator.validRegistrationId(registrationId)) {
            throw new IllegalArgumentException("Invalid registration ID: " + registrationId);
          }
          deviceIdentifier = new RegistrationIdDeviceIdentifier(accountIdentifier, deviceId, registrationId);
          transferArchiveKey = getRegistrationIdTransferArchiveKey(accountIdentifier, deviceId, registrationId);
        }

        Optional.ofNullable(waitForTransferArchiveFuturesByDeviceIdentifier.remove(deviceIdentifier))
            .ifPresent(future -> pubSubRedisClient.withConnection(connection -> connection.async().get(transferArchiveKey))
                .whenComplete((transferArchiveJson, throwable) -> {
                  if (throwable != null) {
                    future.completeExceptionally(throwable);
                  } else {
                    handleTransferArchiveAdded(future, transferArchiveJson);
                  }
                }));
      } catch (final IllegalArgumentException e) {
        logger.error("Could not parse device identifier", e);
      }
    } else if (RESTORE_ACCOUNT_REQUEST_KEYSPACE_PATTERN.equalsIgnoreCase(pattern) && "set".equalsIgnoreCase(message)) {
      // The `- 1` here compensates for the '*' in the pattern
      final String token = channel.substring(RESTORE_ACCOUNT_REQUEST_KEYSPACE_PATTERN.length() - 1);

      Optional.ofNullable(waitForRestoreAccountRequestFuturesByToken.remove(token))
          .ifPresent(future -> pubSubRedisClient.withConnection(connection -> connection.async().get(
                  getRestoreAccountRequestKey(token)))
              .whenComplete((requestJson, throwable) -> {
                if (throwable != null) {
                  future.completeExceptionally(throwable);
                } else {
                  handleRestoreAccountRequest(future, requestJson);
                }
              }));
    }
  }

  private static MessageDigest getSha256MessageDigest() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError("Every implementation of the Java platform is required to support the SHA-256 MessageDigest algorithm", e);
    }
  }
}
