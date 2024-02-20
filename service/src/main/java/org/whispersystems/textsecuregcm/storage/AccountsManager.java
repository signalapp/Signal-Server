/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;


import static com.codahale.metrics.MetricRegistry.name;
import static java.util.Objects.requireNonNull;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.protocol.IdentityKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.redis.RedisOperation;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.DestinationDeviceValidator;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;

public class AccountsManager {

  private static final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer createTimer = metricRegistry.timer(name(AccountsManager.class, "create"));
  private static final Timer updateTimer = metricRegistry.timer(name(AccountsManager.class, "update"));
  private static final Timer getByNumberTimer = metricRegistry.timer(name(AccountsManager.class, "getByNumber"));
  private static final Timer getByUsernameHashTimer = metricRegistry.timer(name(AccountsManager.class, "getByUsernameHash"));
  private static final Timer getByUsernameLinkHandleTimer = metricRegistry.timer(name(AccountsManager.class, "getByUsernameLinkHandle"));
  private static final Timer getByUuidTimer = metricRegistry.timer(name(AccountsManager.class, "getByUuid"));
  private static final Timer deleteTimer = metricRegistry.timer(name(AccountsManager.class, "delete"));

  private static final Timer redisSetTimer = metricRegistry.timer(name(AccountsManager.class, "redisSet"));
  private static final Timer redisPniGetTimer = metricRegistry.timer(name(AccountsManager.class, "redisPniGet"));
  private static final Timer redisUuidGetTimer = metricRegistry.timer(name(AccountsManager.class, "redisUuidGet"));
  private static final Timer redisDeleteTimer = metricRegistry.timer(name(AccountsManager.class, "redisDelete"));

  private static final String CREATE_COUNTER_NAME       = name(AccountsManager.class, "createCounter");
  private static final String DELETE_COUNTER_NAME       = name(AccountsManager.class, "deleteCounter");
  private static final String COUNTRY_CODE_TAG_NAME     = "country";
  private static final String DELETION_REASON_TAG_NAME  = "reason";

  @VisibleForTesting
  public static final String USERNAME_EXPERIMENT_NAME  = "usernames";

  private static final Logger logger = LoggerFactory.getLogger(AccountsManager.class);

  private final Accounts accounts;
  private final PhoneNumberIdentifiers phoneNumberIdentifiers;
  private final FaultTolerantRedisCluster cacheCluster;
  private final AccountLockManager accountLockManager;
  private final KeysManager keysManager;
  private final MessagesManager messagesManager;
  private final ProfilesManager profilesManager;
  private final SecureStorageClient secureStorageClient;
  private final SecureValueRecovery2Client secureValueRecovery2Client;
  private final ClientPresenceManager clientPresenceManager;
  private final ExperimentEnrollmentManager experimentEnrollmentManager;
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;
  private final Executor accountLockExecutor;
  private final Executor clientPresenceExecutor;
  private final Clock clock;

  private static final ObjectWriter ACCOUNT_REDIS_JSON_WRITER = SystemMapper.jsonMapper()
      .writer(SystemMapper.excludingField(Account.class, List.of("uuid")));

  // An account that's used at least daily will get reset in the cache at least once per day when its "last seen"
  // timestamp updates; expiring entries after two days will help clear out "zombie" cache entries that are read
  // frequently (e.g. the account is in an active group and receives messages frequently), but aren't actively used by
  // the owner.
  private static final long CACHE_TTL_SECONDS = Duration.ofDays(2).toSeconds();

  private static final Duration USERNAME_HASH_RESERVATION_TTL_MINUTES = Duration.ofMinutes(5);

  private static final int MAX_UPDATE_ATTEMPTS = 10;

  public enum DeletionReason {
    ADMIN_DELETED("admin"),
    EXPIRED      ("expired"),
    USER_REQUEST ("userRequest");

    private final String tagValue;

    DeletionReason(final String tagValue) {
      this.tagValue = tagValue;
    }
  }

  public AccountsManager(final Accounts accounts,
      final PhoneNumberIdentifiers phoneNumberIdentifiers,
      final FaultTolerantRedisCluster cacheCluster,
      final AccountLockManager accountLockManager,
      final KeysManager keysManager,
      final MessagesManager messagesManager,
      final ProfilesManager profilesManager,
      final SecureStorageClient secureStorageClient,
      final SecureValueRecovery2Client secureValueRecovery2Client,
      final ClientPresenceManager clientPresenceManager,
      final ExperimentEnrollmentManager experimentEnrollmentManager,
      final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
      final Executor accountLockExecutor,
      final Executor clientPresenceExecutor,
      final Clock clock) {
    this.accounts = accounts;
    this.phoneNumberIdentifiers = phoneNumberIdentifiers;
    this.cacheCluster = cacheCluster;
    this.accountLockManager = accountLockManager;
    this.keysManager = keysManager;
    this.messagesManager = messagesManager;
    this.profilesManager = profilesManager;
    this.secureStorageClient = secureStorageClient;
    this.secureValueRecovery2Client = secureValueRecovery2Client;
    this.clientPresenceManager = clientPresenceManager;
    this.experimentEnrollmentManager = experimentEnrollmentManager;
    this.registrationRecoveryPasswordsManager = requireNonNull(registrationRecoveryPasswordsManager);
    this.accountLockExecutor = accountLockExecutor;
    this.clientPresenceExecutor = clientPresenceExecutor;
    this.clock = requireNonNull(clock);
  }

  public Account create(final String number,
      final AccountAttributes accountAttributes,
      final List<AccountBadge> accountBadges,
      final IdentityKey aciIdentityKey,
      final IdentityKey pniIdentityKey,
      final DeviceSpec primaryDeviceSpec) throws InterruptedException {

    final Account account = new Account();

    try (Timer.Context ignoredTimerContext = createTimer.time()) {
      accountLockManager.withLock(List.of(number), () -> {
        final Optional<UUID> maybeRecentlyDeletedAccountIdentifier =
            accounts.findRecentlyDeletedAccountIdentifier(number);

        // Reuse the ACI from any recently-deleted account with this number to cover cases where somebody is
        // re-registering.
        account.setNumber(number, phoneNumberIdentifiers.getPhoneNumberIdentifier(number));
        account.setUuid(maybeRecentlyDeletedAccountIdentifier.orElseGet(UUID::randomUUID));
        account.setIdentityKey(aciIdentityKey);
        account.setPhoneNumberIdentityKey(pniIdentityKey);
        account.addDevice(primaryDeviceSpec.toDevice(Device.PRIMARY_ID, clock));
        account.setRegistrationLockFromAttributes(accountAttributes);
        account.setUnidentifiedAccessKey(accountAttributes.getUnidentifiedAccessKey());
        account.setUnrestrictedUnidentifiedAccess(accountAttributes.isUnrestrictedUnidentifiedAccess());
        account.setDiscoverableByPhoneNumber(accountAttributes.isDiscoverableByPhoneNumber());
        account.setBadges(clock, accountBadges);

        String accountCreationType = maybeRecentlyDeletedAccountIdentifier.isPresent() ? "recently-deleted" : "new";

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

          final UUID aci = e.getExistingAccount().getIdentifier(IdentityType.ACI);
          final UUID pni = e.getExistingAccount().getIdentifier(IdentityType.PNI);

          account.setUuid(aci);
          account.setNumber(e.getExistingAccount().getNumber(), pni);

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
                  profilesManager.deleteAll(aci))
              .thenRunAsync(() -> clientPresenceManager.disconnectAllPresencesForUuid(aci), clientPresenceExecutor)
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
                        profilesManager.deleteAll(aci));
              })
              .join();
        }

        redisSet(account);

        Metrics.counter(CREATE_COUNTER_NAME, "type", accountCreationType).increment();

        accountAttributes.recoveryPassword().ifPresent(registrationRecoveryPassword ->
            registrationRecoveryPasswordsManager.storeForCurrentNumber(account.getNumber(), registrationRecoveryPassword));
      }, accountLockExecutor);

      return account;
    }
  }

  public CompletableFuture<Pair<Account, Device>> addDevice(final Account account, final DeviceSpec deviceSpec) {
    return accountLockManager.withLockAsync(List.of(account.getNumber()),
        () -> addDevice(account.getIdentifier(IdentityType.ACI), deviceSpec, MAX_UPDATE_ATTEMPTS),
        accountLockExecutor);
  }

  private CompletableFuture<Pair<Account, Device>> addDevice(final UUID accountIdentifier, final DeviceSpec deviceSpec, final int retries) {
    return accounts.getByAccountIdentifierAsync(accountIdentifier)
        .thenApply(maybeAccount -> maybeAccount.orElseThrow(ContestedOptimisticLockException::new))
        .thenCompose(account -> {
          final byte nextDeviceId = account.getNextDeviceId();
          account.addDevice(deviceSpec.toDevice(nextDeviceId, clock));

          final List<TransactWriteItem> additionalWriteItems = keysManager.buildWriteItemsForNewDevice(
              account.getIdentifier(IdentityType.ACI),
              account.getIdentifier(IdentityType.PNI),
              nextDeviceId,
              deviceSpec.aciSignedPreKey(),
              deviceSpec.pniSignedPreKey(),
              deviceSpec.aciPqLastResortPreKey(),
              deviceSpec.pniPqLastResortPreKey());

          return CompletableFuture.allOf(
                  keysManager.deleteSingleUsePreKeys(account.getUuid(), nextDeviceId),
                  keysManager.deleteSingleUsePreKeys(account.getPhoneNumberIdentifier(), nextDeviceId),
                  messagesManager.clear(account.getUuid(), nextDeviceId))
              .thenCompose(ignored -> accounts.updateTransactionallyAsync(account, additionalWriteItems))
              .thenApply(ignored -> new Pair<>(account, account.getDevice(nextDeviceId).orElseThrow()));
        })
        .thenCompose(updatedAccountAndDevice -> redisDeleteAsync(updatedAccountAndDevice.first())
            .thenApply(ignored -> updatedAccountAndDevice))
        .exceptionallyCompose(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof ContestedOptimisticLockException && retries > 0) {
            return addDevice(accountIdentifier, deviceSpec, retries - 1);
          }

          return CompletableFuture.failedFuture(throwable);
        });
  }

  public CompletableFuture<Account> removeDevice(final Account account, final byte deviceId) {
    if (deviceId == Device.PRIMARY_ID) {
      throw new IllegalArgumentException("Cannot remove primary device");
    }

    return accountLockManager.withLockAsync(List.of(account.getNumber()),
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

          return accounts.updateTransactionallyAsync(account, keysManager.buildWriteItemsForRemovedDevice(
              account.getIdentifier(IdentityType.ACI),
              account.getIdentifier(IdentityType.PNI),
              deviceId))
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
        .whenCompleteAsync((ignored, throwable) -> {
          if (throwable == null) {
            RedisOperation.unchecked(() -> clientPresenceManager.disconnectPresence(accountIdentifier, deviceId));
          }
        }, clientPresenceExecutor);
  }

  public Account changeNumber(final Account account,
                              final String targetNumber,
                              @Nullable final IdentityKey pniIdentityKey,
      @Nullable final Map<Byte, ECSignedPreKey> pniSignedPreKeys,
      @Nullable final Map<Byte, KEMSignedPreKey> pniPqLastResortPreKeys,
      @Nullable final Map<Byte, Integer> pniRegistrationIds) throws InterruptedException, MismatchedDevicesException {

    final String originalNumber = account.getNumber();
    final UUID originalPhoneNumberIdentifier = account.getPhoneNumberIdentifier();

    if (originalNumber.equals(targetNumber)) {
      if (pniIdentityKey != null) {
        throw new IllegalArgumentException("change number must supply a changed phone number; otherwise use updatePniKeys");
      }
      return account;
    }

    validateDevices(account, pniSignedPreKeys, pniPqLastResortPreKeys, pniRegistrationIds);

    final AtomicReference<Account> updatedAccount = new AtomicReference<>();

    accountLockManager.withLock(List.of(account.getNumber(), targetNumber), () -> {
      redisDelete(account);

      // There are three possible states for accounts associated with the target phone number:
      //
      // 1. An account exists with the target number; the caller has proved ownership of the number, so delete the
      //    account with the target number. This will leave a "deleted account" record for the deleted account mapping
      //    the UUID of the deleted account to the target phone number. We'll then overwrite that so it points to the
      //    original number to facilitate switching back and forth between numbers.
      // 2. No account with the target number exists, but one has recently been deleted. In that case, add a "deleted
      //    account" record that maps the ACI of the recently-deleted account to the now-abandoned original phone number
      //    of the account changing its number (which facilitates ACI consistency in cases that a party is switching
      //    back and forth between numbers).
      // 3. No account with the target number exists at all, in which case no additional action is needed.
      final Optional<UUID> recentlyDeletedAci = accounts.findRecentlyDeletedAccountIdentifier(targetNumber);
      final Optional<Account> maybeExistingAccount = getByE164(targetNumber);
      final Optional<UUID> maybeDisplacedUuid;

      if (maybeExistingAccount.isPresent()) {
        delete(maybeExistingAccount.get()).join();
        maybeDisplacedUuid = maybeExistingAccount.map(Account::getUuid);
      } else {
        maybeDisplacedUuid = recentlyDeletedAci;
      }

      final UUID uuid = account.getUuid();
      final UUID phoneNumberIdentifier = phoneNumberIdentifiers.getPhoneNumberIdentifier(targetNumber);

      CompletableFuture.allOf(
              keysManager.deleteSingleUsePreKeys(phoneNumberIdentifier),
              keysManager.deleteSingleUsePreKeys(originalPhoneNumberIdentifier))
          .join();

      final Collection<TransactWriteItem> keyWriteItems =
          buildPniKeyWriteItems(uuid, phoneNumberIdentifier, pniSignedPreKeys, pniPqLastResortPreKeys);

      final Account numberChangedAccount = updateWithRetries(
          account,
          a -> {
            setPniKeys(account, pniIdentityKey, pniRegistrationIds);
            return true;
          },
          a -> accounts.changeNumber(a, targetNumber, phoneNumberIdentifier, maybeDisplacedUuid, keyWriteItems),
          () -> accounts.getByAccountIdentifier(uuid).orElseThrow(),
          AccountChangeValidator.NUMBER_CHANGE_VALIDATOR);

      updatedAccount.set(numberChangedAccount);
    }, accountLockExecutor);

    return updatedAccount.get();
  }

  public Account updatePniKeys(final Account account,
      final IdentityKey pniIdentityKey,
      final Map<Byte, ECSignedPreKey> pniSignedPreKeys,
      @Nullable final Map<Byte, KEMSignedPreKey> pniPqLastResortPreKeys,
      final Map<Byte, Integer> pniRegistrationIds) throws MismatchedDevicesException {

    validateDevices(account, pniSignedPreKeys, pniPqLastResortPreKeys, pniRegistrationIds);

    final UUID aci = account.getIdentifier(IdentityType.ACI);
    final UUID pni = account.getIdentifier(IdentityType.PNI);

    final Collection<TransactWriteItem> keyWriteItems =
        buildPniKeyWriteItems(pni, pni, pniSignedPreKeys, pniPqLastResortPreKeys);

    return redisDeleteAsync(account)
        .thenCompose(ignored -> keysManager.deleteSingleUsePreKeys(pni))
        .thenCompose(ignored -> updateTransactionallyWithRetriesAsync(account,
            a -> setPniKeys(a, pniIdentityKey, pniRegistrationIds),
            accounts::updateTransactionallyAsync,
            () -> accounts.getByAccountIdentifierAsync(aci).thenApply(Optional::orElseThrow),
            a -> keyWriteItems,
            AccountChangeValidator.GENERAL_CHANGE_VALIDATOR,
            MAX_UPDATE_ATTEMPTS))
        .join();
  }

  private Collection<TransactWriteItem> buildPniKeyWriteItems(
      final UUID enabledDevicesIdentifier,
      final UUID phoneNumberIdentifier,
      @Nullable final Map<Byte, ECSignedPreKey> pniSignedPreKeys,
      @Nullable final Map<Byte, KEMSignedPreKey> pniPqLastResortPreKeys) {

    final List<TransactWriteItem> keyWriteItems = new ArrayList<>();

    if (pniSignedPreKeys != null) {
      pniSignedPreKeys.forEach((deviceId, signedPreKey) ->
          keyWriteItems.add(keysManager.buildWriteItemForEcSignedPreKey(phoneNumberIdentifier, deviceId, signedPreKey)));
    }

    if (pniPqLastResortPreKeys != null) {
      keysManager.getPqEnabledDevices(enabledDevicesIdentifier)
          .thenAccept(deviceIds -> deviceIds.stream()
              .filter(pniPqLastResortPreKeys::containsKey)
              .map(deviceId -> keysManager.buildWriteItemForLastResortKey(phoneNumberIdentifier,
                  deviceId,
                  pniPqLastResortPreKeys.get(deviceId)))
              .forEach(keyWriteItems::add))
          .join();
    }

    return keyWriteItems;
  }

  private void setPniKeys(final Account account,
      @Nullable final IdentityKey pniIdentityKey,
      @Nullable final Map<Byte, Integer> pniRegistrationIds) {

    if (ObjectUtils.allNull(pniIdentityKey, pniRegistrationIds)) {
      return;
    } else if (!ObjectUtils.allNotNull(pniIdentityKey, pniRegistrationIds)) {
      throw new IllegalArgumentException("PNI identity key and registration IDs must be all null or all non-null");
    }

    account.getDevices()
        .stream()
        .filter(Device::isEnabled)
        .forEach(device -> device.setPhoneNumberIdentityRegistrationId(pniRegistrationIds.get(device.getId())));

    account.setPhoneNumberIdentityKey(pniIdentityKey);
  }

  private void validateDevices(final Account account,
      @Nullable final Map<Byte, ECSignedPreKey> pniSignedPreKeys,
      @Nullable final Map<Byte, KEMSignedPreKey> pniPqLastResortPreKeys,
      @Nullable final Map<Byte, Integer> pniRegistrationIds) throws MismatchedDevicesException {
    if (pniSignedPreKeys == null && pniRegistrationIds == null) {
      return;
    } else if (pniSignedPreKeys == null || pniRegistrationIds == null) {
      throw new IllegalArgumentException("Signed pre-keys and registration IDs must both be null or both be non-null");
    }

    // Check that all including primary ID are in signed pre-keys
    DestinationDeviceValidator.validateCompleteDeviceList(
        account,
        pniSignedPreKeys.keySet(),
        Collections.emptySet());

    // Check that all including primary ID are in Pq pre-keys
    if (pniPqLastResortPreKeys != null) {
      DestinationDeviceValidator.validateCompleteDeviceList(
          account,
          pniPqLastResortPreKeys.keySet(),
          Collections.emptySet());
    }

    // Check that all devices are accounted for in the map of new PNI registration IDs
    DestinationDeviceValidator.validateCompleteDeviceList(
        account,
        pniRegistrationIds.keySet(),
        Collections.emptySet());
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
    if (!experimentEnrollmentManager.isEnrolled(account.getUuid(), USERNAME_EXPERIMENT_NAME)) {
      return CompletableFuture.failedFuture(new UsernameHashNotAvailableException());
    }

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
    if (!experimentEnrollmentManager.isEnrolled(account.getUuid(), USERNAME_EXPERIMENT_NAME)) {
      return CompletableFuture.failedFuture(new UsernameHashNotAvailableException());
    }

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

    final Account updatedAccount;

    try (Timer.Context ignored = updateTimer.time()) {

      redisDelete(account);

      final UUID uuid = account.getUuid();

      updatedAccount = updateWithRetries(account,
          updater,
          accounts::update,
          () -> accounts.getByAccountIdentifier(uuid).orElseThrow(),
          AccountChangeValidator.GENERAL_CHANGE_VALIDATOR);

      redisSet(updatedAccount);
    }

    return updatedAccount;
  }

  private CompletableFuture<Account> updateAsync(final Account account, final Function<Account, Boolean> updater) {

    final Timer.Context timerContext = updateTimer.time();

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
        .whenComplete((ignored, throwable) -> timerContext.close());
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

  private CompletionStage<Account> updateTransactionallyWithRetriesAsync(final Account account,
      final Consumer<Account> updater,
      final BiFunction<Account, Collection<TransactWriteItem>, CompletionStage<Void>> persister,
      final Supplier<CompletionStage<Account>> retriever,
      final Function<Account, Collection<TransactWriteItem>> additionalWriteItemProvider,
      final AccountChangeValidator changeValidator,
      final int remainingTries) {

    final Account originalAccount = AccountUtil.cloneAccountAsNotStale(account);

    final Collection<TransactWriteItem> additionalWriteItems = additionalWriteItemProvider.apply(account);
    updater.accept(account);

    if (remainingTries > 0) {
      return persister.apply(account, additionalWriteItems)
          .thenApply(ignored -> {
            final Account updatedAccount = AccountUtil.cloneAccountAsNotStale(account);
            account.markStale();

            changeValidator.validateChange(originalAccount, updatedAccount);

            return updatedAccount;
          })
          .exceptionallyCompose(throwable -> {
            if (ExceptionUtils.unwrap(throwable) instanceof ContestedOptimisticLockException) {
              return retriever.get().thenCompose(refreshedAccount ->
                  updateTransactionallyWithRetriesAsync(refreshedAccount, updater, persister, retriever, additionalWriteItemProvider, changeValidator, remainingTries - 1));
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
    return getByNumberTimer.timeSupplier(() -> accounts.getByE164(number));
  }

  public CompletableFuture<Optional<Account>> getByE164Async(final String number) {
    final Timer.Context context = getByNumberTimer.time();
    return accounts.getByE164Async(number)
        .whenComplete((ignoredResult, ignoredThrowable) -> context.close());
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
    final Timer.Context context = getByUsernameLinkHandleTimer.time();
    return accounts.getByUsernameLinkHandle(usernameLinkHandle)
        .whenComplete((ignoredResult, ignoredThrowable) -> context.close());
  }

  public CompletableFuture<Optional<Account>> getByUsernameHash(final byte[] usernameHash) {
    final Timer.Context context = getByUsernameHashTimer.time();
    return accounts.getByUsernameHash(usernameHash)
        .whenComplete((ignoredResult, ignoredThrowable) -> context.close());
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
    return phoneNumberIdentifiers.getPhoneNumberIdentifier(e164);
  }

  public Optional<UUID> findRecentlyDeletedAccountIdentifier(final String e164) {
    return accounts.findRecentlyDeletedAccountIdentifier(e164);
  }

  public Optional<String> findRecentlyDeletedE164(final UUID uuid) {
    return accounts.findRecentlyDeletedE164(uuid);
  }

  public Flux<Account> streamAllFromDynamo(final int segments, final Scheduler scheduler) {
    return accounts.getAll(segments, scheduler);
  }

  public CompletableFuture<Void> delete(final Account account, final DeletionReason deletionReason) {
    @SuppressWarnings("resource") final Timer.Context timerContext = deleteTimer.time();

    return accountLockManager.withLockAsync(List.of(account.getNumber()), () -> delete(account), accountLockExecutor)
        .whenComplete((ignored, throwable) -> {
          timerContext.close();

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
    final List<TransactWriteItem> additionalWriteItems =
        account.getDevices().stream().flatMap(device -> keysManager.buildWriteItemsForRemovedDevice(
                account.getIdentifier(IdentityType.ACI),
                account.getIdentifier(IdentityType.PNI),
                device.getId()).stream())
            .toList();

    return CompletableFuture.allOf(
            secureStorageClient.deleteStoredData(account.getUuid()),
            secureValueRecovery2Client.deleteBackups(account.getUuid()),
            keysManager.deleteSingleUsePreKeys(account.getUuid()),
            keysManager.deleteSingleUsePreKeys(account.getPhoneNumberIdentifier()),
            messagesManager.clear(account.getUuid()),
            messagesManager.clear(account.getPhoneNumberIdentifier()),
            profilesManager.deleteAll(account.getUuid()),
            registrationRecoveryPasswordsManager.removeForNumber(account.getNumber()))
        .thenCompose(ignored -> accounts.delete(account.getUuid(), additionalWriteItems))
        .thenCompose(ignored -> redisDeleteAsync(account))
        .thenRunAsync(() -> RedisOperation.unchecked(() ->
            account.getDevices().forEach(device ->
                clientPresenceManager.disconnectPresence(account.getUuid(), device.getId()))), clientPresenceExecutor);
  }

  private String getUsernameHashAccountMapKey(byte[] usernameHash) {
    return "UAccountMap::" + Base64.getUrlEncoder().withoutPadding().encodeToString(usernameHash);
  }

  private String getAccountMapKey(String key) {
    return "AccountMap::" + key;
  }

  private String getAccountEntityKey(UUID uuid) {
    return "Account3::" + uuid.toString();
  }

  private void redisSet(Account account) {
    try (Timer.Context ignored = redisSetTimer.time()) {
      final String accountJson = writeRedisAccountJson(account);

      cacheCluster.useCluster(connection -> {
        final RedisAdvancedClusterCommands<String, String> commands = connection.sync();

        commands.setex(getAccountMapKey(account.getPhoneNumberIdentifier().toString()), CACHE_TTL_SECONDS, account.getUuid().toString());
        commands.setex(getAccountEntityKey(account.getUuid()), CACHE_TTL_SECONDS, accountJson);
      });
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
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
    try (final Timer.Context ignored = overallTimer.time()) {
      Optional<Account> account = resolveFromRedis.get();
      if (account.isEmpty()) {
        account = resolveFromAccounts.get();
        account.ifPresent(this::redisSet);
      }
      return account;
    }
  }

  private CompletableFuture<Optional<Account>> checkRedisThenAccountsAsync(
      final Timer overallTimer,
      final Supplier<CompletableFuture<Optional<Account>>> resolveFromRedis,
      final Supplier<CompletableFuture<Optional<Account>>> resolveFromAccounts) {

    @SuppressWarnings("resource") final Timer.Context timerContext = overallTimer.time();

    return resolveFromRedis.get()
        .thenCompose(maybeAccountFromRedis -> maybeAccountFromRedis
            .map(accountFromRedis -> CompletableFuture.completedFuture(maybeAccountFromRedis))
            .orElseGet(() -> resolveFromAccounts.get()
                .thenCompose(maybeAccountFromAccounts -> maybeAccountFromAccounts
                    .map(account -> redisSetAsync(account).thenApply(ignored -> maybeAccountFromAccounts))
                    .orElseGet(() -> CompletableFuture.completedFuture(maybeAccountFromAccounts)))))
        .whenComplete((ignored, throwable) -> timerContext.close());
  }

  private Optional<Account> redisGetBySecondaryKey(final String secondaryKey, final Timer timer) {
    try (final Timer.Context ignored = timer.time()) {
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
  }

  private CompletableFuture<Optional<Account>> redisGetBySecondaryKeyAsync(final String secondaryKey, final Timer timer) {
    @SuppressWarnings("resource") final Timer.Context timerContext = timer.time();

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
        .whenComplete((ignored, throwable) -> timerContext.close())
        .toCompletableFuture();
  }

  private Optional<Account> redisGetByAccountIdentifier(UUID uuid) {
    try (Timer.Context ignored = redisUuidGetTimer.time()) {
      final String json = cacheCluster.withCluster(connection -> connection.sync().get(getAccountEntityKey(uuid)));

      return parseAccountJson(json, uuid);
    } catch (final RedisException e) {
      logger.warn("Redis failure", e);
      return Optional.empty();
    }
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
    try (final Timer.Context ignored = redisDeleteTimer.time()) {
      cacheCluster.useCluster(connection -> {
        connection.sync().del(
            getAccountMapKey(account.getPhoneNumberIdentifier().toString()),
            getAccountEntityKey(account.getUuid()));
      });
    }
  }

  private CompletableFuture<Void> redisDeleteAsync(final Account account) {
    @SuppressWarnings("resource") final Timer.Context timerContext = redisDeleteTimer.time();

    final String[] keysToDelete = new String[]{
        getAccountMapKey(account.getPhoneNumberIdentifier().toString()),
        getAccountEntityKey(account.getUuid())
    };

    return cacheCluster.withCluster(connection -> connection.async().del(keysToDelete))
        .toCompletableFuture()
        .whenComplete((ignoredResult, ignoredException) -> timerContext.close())
        .thenRun(Util.NOOP);
  }
}
