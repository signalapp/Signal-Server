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
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
import org.whispersystems.textsecuregcm.securebackup.SecureBackupClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecovery2Client;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.DestinationDeviceValidator;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;

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
  private static final Timer redisNumberGetTimer = metricRegistry.timer(name(AccountsManager.class, "redisNumberGet"));
  private static final Timer redisUsernameHashGetTimer = metricRegistry.timer(name(AccountsManager.class, "redisUsernameHashGet"));
  private static final Timer redisUsernameLinkHandleGetTimer = metricRegistry.timer(name(AccountsManager.class, "redisUsernameLinkHandleGet"));
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
  private final SecureBackupClient secureBackupClient;
  private final SecureValueRecovery2Client secureValueRecovery2Client;
  private final ClientPresenceManager clientPresenceManager;
  private final ExperimentEnrollmentManager experimentEnrollmentManager;
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;
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

  @FunctionalInterface
  private interface AccountPersister {
    void persistAccount(Account account) throws UsernameHashNotAvailableException;
  }

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
      final SecureBackupClient secureBackupClient,
      final SecureValueRecovery2Client secureValueRecovery2Client,
      final ClientPresenceManager clientPresenceManager,
      final ExperimentEnrollmentManager experimentEnrollmentManager,
      final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
      final Clock clock) {
    this.accounts = accounts;
    this.phoneNumberIdentifiers = phoneNumberIdentifiers;
    this.cacheCluster = cacheCluster;
    this.accountLockManager = accountLockManager;
    this.keysManager = keysManager;
    this.messagesManager = messagesManager;
    this.profilesManager = profilesManager;
    this.secureStorageClient = secureStorageClient;
    this.secureBackupClient = secureBackupClient;
    this.secureValueRecovery2Client = secureValueRecovery2Client;
    this.clientPresenceManager = clientPresenceManager;
    this.experimentEnrollmentManager = experimentEnrollmentManager;
    this.registrationRecoveryPasswordsManager = requireNonNull(registrationRecoveryPasswordsManager);
    this.clock = requireNonNull(clock);
  }

  public Account create(final String number,
      final String password,
      final String signalAgent,
      final AccountAttributes accountAttributes,
      final List<AccountBadge> accountBadges) throws InterruptedException {

    try (Timer.Context ignored = createTimer.time()) {
      final Account account = new Account();

      accountLockManager.withLock(List.of(number), () -> {
        Device device = new Device();
        device.setId(Device.MASTER_ID);
        device.setAuthTokenHash(SaltedTokenHash.generateFor(password));
        device.setFetchesMessages(accountAttributes.getFetchesMessages());
        device.setRegistrationId(accountAttributes.getRegistrationId());
        accountAttributes.getPhoneNumberIdentityRegistrationId().ifPresent(device::setPhoneNumberIdentityRegistrationId);
        device.setName(accountAttributes.getName());
        device.setCapabilities(accountAttributes.getCapabilities());
        device.setCreated(System.currentTimeMillis());
        device.setLastSeen(Util.todayInMillis());
        device.setUserAgent(signalAgent);

        account.setNumber(number, phoneNumberIdentifiers.getPhoneNumberIdentifier(number));

        // Reuse the ACI from any recently-deleted account with this number to cover cases where somebody is
        // re-registering.
        account.setUuid(accounts.findRecentlyDeletedAccountIdentifier(number).orElseGet(UUID::randomUUID));
        account.addDevice(device);
        account.setRegistrationLockFromAttributes(accountAttributes);
        account.setUnidentifiedAccessKey(accountAttributes.getUnidentifiedAccessKey());
        account.setUnrestrictedUnidentifiedAccess(accountAttributes.isUnrestrictedUnidentifiedAccess());
        account.setDiscoverableByPhoneNumber(accountAttributes.isDiscoverableByPhoneNumber());
        account.setBadges(clock, accountBadges);

        final UUID originalUuid = account.getUuid();

        boolean freshUser = accounts.create(account);

        // create() sometimes updates the UUID, if there was a number conflict.
        // for metrics, we want secondary to run with the same original UUID
        final UUID actualUuid = account.getUuid();

        redisSet(account);

        // In terms of previously-existing accounts, there are three possible cases:
        //
        // 1. This is a completely new account; there was no pre-existing account and no recently-deleted account
        // 2. This is a re-registration of an existing account. The storage layer will update the existing account in
        //    place to match the account record created above, and will update the UUID of the newly-created account
        //    instance to match the stored account record (i.e. originalUuid != actualUuid).
        // 3. This is a re-registration of a recently-deleted account, in which case maybeRecentlyDeletedUuid is
        //    present.
        //
        // All cases are mutually-exclusive. In the first case, we don't need to do anything. In the third, we can be
        // confident that everything has already been deleted. In the second case, though, we're taking over an existing
        // account and need to clear out messages and keys that may have been stored for the old account.
        if (!originalUuid.equals(actualUuid)) {
          final CompletableFuture<Void> deleteKeysFuture = CompletableFuture.allOf(
              keysManager.delete(actualUuid),
              keysManager.delete(account.getPhoneNumberIdentifier()));

          messagesManager.clear(actualUuid).join();
          profilesManager.deleteAll(actualUuid);

          deleteKeysFuture.join();

          clientPresenceManager.disconnectAllPresencesForUuid(actualUuid);
        }

        final Tags tags;

        if (freshUser) {
          tags = Tags.of("type", "new");
        } else if (!originalUuid.equals(actualUuid)) {
          tags = Tags.of("type", "re-registration");
        } else {
          tags = Tags.of("type", "recently-deleted");
        }

        Metrics.counter(CREATE_COUNTER_NAME, tags).increment();

        accountAttributes.recoveryPassword().ifPresent(registrationRecoveryPassword ->
            registrationRecoveryPasswordsManager.storeForCurrentNumber(account.getNumber(), registrationRecoveryPassword));
      });

      return account;
    }
  }

  public Account changeNumber(final Account account,
                              final String targetNumber,
                              @Nullable final IdentityKey pniIdentityKey,
                              @Nullable final Map<Long, ECSignedPreKey> pniSignedPreKeys,
                              @Nullable final Map<Long, KEMSignedPreKey> pniPqLastResortPreKeys,
                              @Nullable final Map<Long, Integer> pniRegistrationIds) throws InterruptedException, MismatchedDevicesException {

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
        delete(maybeExistingAccount.get());
        maybeDisplacedUuid = maybeExistingAccount.map(Account::getUuid);
      } else {
        maybeDisplacedUuid = recentlyDeletedAci;
      }

      final UUID uuid = account.getUuid();
      final UUID phoneNumberIdentifier = phoneNumberIdentifiers.getPhoneNumberIdentifier(targetNumber);

      final Account numberChangedAccount;

      numberChangedAccount = updateWithRetries(
          account,
          a -> {
            setPniKeys(account, pniIdentityKey, pniSignedPreKeys, pniRegistrationIds);
            return true;
          },
          a -> accounts.changeNumber(a, targetNumber, phoneNumberIdentifier, maybeDisplacedUuid),
          () -> accounts.getByAccountIdentifier(uuid).orElseThrow(),
          AccountChangeValidator.NUMBER_CHANGE_VALIDATOR);

      updatedAccount.set(numberChangedAccount);

      CompletableFuture.allOf(
              keysManager.delete(phoneNumberIdentifier),
              keysManager.delete(originalPhoneNumberIdentifier))
          .join();

      keysManager.storeEcSignedPreKeys(phoneNumberIdentifier, pniSignedPreKeys);

      if (pniPqLastResortPreKeys != null) {
        keysManager.storePqLastResort(
            phoneNumberIdentifier,
            keysManager.getPqEnabledDevices(uuid).join().stream().collect(
                Collectors.toMap(
                    Function.identity(),
                    pniPqLastResortPreKeys::get)));
      }
    });

    return updatedAccount.get();
  }

  public static boolean validNewAccountAttributes(final AccountAttributes accountAttributes) {
    if (!validRegistrationId(accountAttributes.getRegistrationId())) {
      return false;
    }
    final OptionalInt pniRegistrationId = accountAttributes.getPhoneNumberIdentityRegistrationId();
    if (pniRegistrationId.isPresent() && !validRegistrationId(pniRegistrationId.getAsInt())) {
      return false;
    }
    return true;
  }

  private static boolean validRegistrationId(int registrationId) {
    return registrationId > 0 && registrationId <= Device.MAX_REGISTRATION_ID;
  }

  public Account updatePniKeys(final Account account,
      final IdentityKey pniIdentityKey,
      final Map<Long, ECSignedPreKey> pniSignedPreKeys,
      @Nullable final Map<Long, KEMSignedPreKey> pniPqLastResortPreKeys,
      final Map<Long, Integer> pniRegistrationIds) throws MismatchedDevicesException {
    validateDevices(account, pniSignedPreKeys, pniPqLastResortPreKeys, pniRegistrationIds);

    final UUID pni = account.getPhoneNumberIdentifier();
    final Account updatedAccount = update(account, a -> { return setPniKeys(a, pniIdentityKey, pniSignedPreKeys, pniRegistrationIds); });

    final List<Long> pqEnabledDeviceIDs = keysManager.getPqEnabledDevices(pni).join();
    keysManager.delete(pni);
    keysManager.storeEcSignedPreKeys(pni, pniSignedPreKeys).join();
    if (pniPqLastResortPreKeys != null && !pqEnabledDeviceIDs.isEmpty()) {
      keysManager.storePqLastResort(pni, pqEnabledDeviceIDs.stream().collect(Collectors.toMap(Function.identity(), pniPqLastResortPreKeys::get))).join();
    }

    return updatedAccount;
  }

  private boolean setPniKeys(final Account account,
      @Nullable final IdentityKey pniIdentityKey,
      @Nullable final Map<Long, ECSignedPreKey> pniSignedPreKeys,
      @Nullable final Map<Long, Integer> pniRegistrationIds) {
    if (ObjectUtils.allNull(pniIdentityKey, pniSignedPreKeys, pniRegistrationIds)) {
      return false;
    } else if (!ObjectUtils.allNotNull(pniIdentityKey, pniSignedPreKeys, pniRegistrationIds)) {
      throw new IllegalArgumentException("PNI identity key, signed pre-keys, and registration IDs must be all null or all non-null");
    }

    boolean changed = !Objects.equals(pniIdentityKey, account.getIdentityKey(IdentityType.PNI));

    for (Device device : account.getDevices()) {
        if (!device.isEnabled()) {
            continue;
        }
        ECSignedPreKey signedPreKey = pniSignedPreKeys.get(device.getId());
        int registrationId = pniRegistrationIds.get(device.getId());
        changed = changed ||
            !signedPreKey.equals(device.getSignedPreKey(IdentityType.PNI)) ||
            device.getRegistrationId() != registrationId;
        device.setPhoneNumberIdentitySignedPreKey(signedPreKey);
        device.setPhoneNumberIdentityRegistrationId(registrationId);
    }

    account.setPhoneNumberIdentityKey(pniIdentityKey);

    return changed;
  }

  private void validateDevices(final Account account,
      @Nullable final Map<Long, ECSignedPreKey> pniSignedPreKeys,
      @Nullable final Map<Long, KEMSignedPreKey> pniPqLastResortPreKeys,
      @Nullable final Map<Long, Integer> pniRegistrationIds) throws MismatchedDevicesException {
    if (pniSignedPreKeys == null && pniRegistrationIds == null) {
      return;
    } else if (pniSignedPreKeys == null || pniRegistrationIds == null) {
      throw new IllegalArgumentException("Signed pre-keys and registration IDs must both be null or both be non-null");
    }

    // Check that all including master ID are in signed pre-keys
    DestinationDeviceValidator.validateCompleteDeviceList(
        account,
        pniSignedPreKeys.keySet(),
        Collections.emptySet());

    // Check that all including master ID are in Pq pre-keys
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
   * @return the reserved username hash and an updated Account object
   * @throws UsernameHashNotAvailableException no username hash is available
   */
  public UsernameReservation reserveUsernameHash(final Account account, final List<byte[]> requestedUsernameHashes) throws UsernameHashNotAvailableException {
    if (!experimentEnrollmentManager.isEnrolled(account.getUuid(), USERNAME_EXPERIMENT_NAME)) {
      throw new UsernameHashNotAvailableException();
    }

    redisDelete(account);

    class Reserver implements AccountPersister {
      byte[] reservedUsernameHash;

      @Override
      public void persistAccount(final Account account) throws UsernameHashNotAvailableException {
        for (byte[] usernameHash : requestedUsernameHashes) {
          if (accounts.usernameHashAvailable(usernameHash)) {
            reservedUsernameHash = usernameHash;
            accounts.reserveUsernameHash(
              account,
              usernameHash,
              USERNAME_HASH_RESERVATION_TTL_MINUTES);
            return;
          }
        }
        throw new UsernameHashNotAvailableException();
      }
    }
    final Reserver reserver = new Reserver();
    final Account updatedAccount = failableUpdateWithRetries(
        account,
        a -> true,
        reserver,
        () -> accounts.getByAccountIdentifier(account.getUuid()).orElseThrow(),
        AccountChangeValidator.USERNAME_CHANGE_VALIDATOR);
    return new UsernameReservation(updatedAccount, reserver.reservedUsernameHash);
  }

  /**
   * Set a username hash previously reserved with {@link #reserveUsernameHash(Account, List)}
   *
   * @param account the account to update
   * @param reservedUsernameHash the previously reserved username hash
   * @param encryptedUsername the encrypted form of the previously reserved username for the username link
   * @return the updated account with the username hash field set
   * @throws UsernameHashNotAvailableException if the reserved username hash has been taken (because the reservation expired)
   * @throws UsernameReservationNotFoundException if `reservedUsernameHash` was not reserved in the account
   */
  public Account confirmReservedUsernameHash(final Account account, final byte[] reservedUsernameHash, @Nullable final byte[] encryptedUsername) throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    if (!experimentEnrollmentManager.isEnrolled(account.getUuid(), USERNAME_EXPERIMENT_NAME)) {
      throw new UsernameHashNotAvailableException();
    }
    if (account.getUsernameHash().map(currentUsernameHash -> Arrays.equals(currentUsernameHash, reservedUsernameHash)).orElse(false)) {
      // the client likely already succeeded and is retrying
      return account;
    }

    if (!account.getReservedUsernameHash().map(oldHash -> Arrays.equals(oldHash, reservedUsernameHash)).orElse(false)) {
      // no such reservation existed, either there was no previous call to reserveUsername
      // or the reservation changed
      throw new UsernameReservationNotFoundException();
    }

    redisDelete(account);

    return failableUpdateWithRetries(
        account,
        a -> true,
        a -> {
          // though we know this username hash was reserved, the reservation could have lapsed
          if (!accounts.usernameHashAvailable(Optional.of(account.getUuid()), reservedUsernameHash)) {
            throw new UsernameHashNotAvailableException();
          }
          accounts.confirmUsernameHash(a, reservedUsernameHash, encryptedUsername);
        },
        () -> accounts.getByAccountIdentifier(account.getUuid()).orElseThrow(),
        AccountChangeValidator.USERNAME_CHANGE_VALIDATOR);
  }

  public Account clearUsernameHash(final Account account) {
    redisDelete(account);

    return updateWithRetries(
        account,
        a -> true,
        accounts::clearUsernameHash,
        () -> accounts.getByAccountIdentifier(account.getUuid()).orElseThrow(),
        AccountChangeValidator.USERNAME_CHANGE_VALIDATOR);
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
   * Specialized version of {@link #updateDevice(Account, long, Consumer)} that minimizes potentially contentious and
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
    try {
      return failableUpdateWithRetries(account, updater, persister::accept, retriever, changeValidator);
    } catch (UsernameHashNotAvailableException e) {
      // not possible
      throw new IllegalStateException(e);
    }
  }

  private Account failableUpdateWithRetries(Account account,
      final Function<Account, Boolean> updater,
      final AccountPersister persister,
      final Supplier<Account> retriever,
      final AccountChangeValidator changeValidator) throws UsernameHashNotAvailableException {

    Account originalAccount = cloneAccountAsNotStale(account);

    if (!updater.apply(account)) {
      return account;
    }

    final int maxTries = 10;
    int tries = 0;

    while (tries < maxTries) {

      try {
        persister.persistAccount(account);

        final Account updatedAccount = cloneAccountAsNotStale(account);
        account.markStale();

        changeValidator.validateChange(originalAccount, updatedAccount);

        return updatedAccount;
      } catch (final ContestedOptimisticLockException e) {
        tries++;

        account = retriever.get();
        originalAccount = cloneAccountAsNotStale(account);

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

    final Account originalAccount = cloneAccountAsNotStale(account);

    if (!updater.apply(account)) {
      return CompletableFuture.completedFuture(account);
    }

    if (remainingTries > 0) {
      return persister.apply(account)
          .thenApply(ignored -> {
            final Account updatedAccount = cloneAccountAsNotStale(account);
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

  private static Account cloneAccountAsNotStale(final Account account) {
    try {
      return SystemMapper.jsonMapper().readValue(
          SystemMapper.jsonMapper().writeValueAsBytes(account), Account.class);
    } catch (final IOException e) {
      // this should really, truly, never happen
      throw new IllegalArgumentException(e);
    }
  }

  public Account updateDevice(Account account, long deviceId, Consumer<Device> deviceUpdater) {
    return update(account, a -> {
      a.getDevice(deviceId).ifPresent(deviceUpdater);
      // assume that all updaters passed to the public method actually modify the device
      return true;
    });
  }

  public CompletableFuture<Account> updateDeviceAsync(final Account account, final long deviceId, final Consumer<Device> deviceUpdater) {
    return updateAsync(account, a -> {
      a.getDevice(deviceId).ifPresent(deviceUpdater);
      // assume that all updaters passed to the public method actually modify the device
      return true;
    });
  }

  public Optional<Account> getByE164(final String number) {
    return checkRedisThenAccounts(
        getByNumberTimer,
        () -> redisGetBySecondaryKey(getAccountMapKey(number), redisNumberGetTimer),
        () -> accounts.getByE164(number)
    );
  }

  public CompletableFuture<Optional<Account>> getByE164Async(final String number) {
    return checkRedisThenAccountsAsync(
        getByNumberTimer,
        () -> redisGetBySecondaryKeyAsync(getAccountMapKey(number), redisNumberGetTimer),
        () -> accounts.getByE164Async(number)
    );
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

  public Optional<Account> getByUsernameLinkHandle(final UUID usernameLinkHandle) {
    return checkRedisThenAccounts(
        getByUsernameLinkHandleTimer,
        () -> redisGetBySecondaryKey(getAccountMapKey(usernameLinkHandle.toString()), redisUsernameLinkHandleGetTimer),
        () -> accounts.getByUsernameLinkHandle(usernameLinkHandle)
    );
  }

  public Optional<Account> getByUsernameHash(final byte[] usernameHash) {
    return checkRedisThenAccounts(
        getByUsernameHashTimer,
        () -> redisGetBySecondaryKey(getUsernameHashAccountMapKey(usernameHash), redisUsernameHashGetTimer),
        () -> accounts.getByUsernameHash(usernameHash)
    );
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

  public AccountCrawlChunk getAllFromDynamo(int length) {
    return accounts.getAllFromStart(length);
  }

  public AccountCrawlChunk getAllFromDynamo(UUID uuid, int length) {
    return accounts.getAllFrom(uuid, length);
  }

  public ParallelFlux<Account> streamAllFromDynamo(final int segments, final Scheduler scheduler) {
    return accounts.getAll(segments, scheduler);
  }

  public void delete(final Account account, final DeletionReason deletionReason) throws InterruptedException {
    try (final Timer.Context ignored = deleteTimer.time()) {
      accountLockManager.withLock(List.of(account.getNumber()), () -> delete(account));
    } catch (final RuntimeException | InterruptedException e) {
      logger.warn("Failed to delete account", e);
      throw e;
    }

    Metrics.counter(DELETE_COUNTER_NAME,
        COUNTRY_CODE_TAG_NAME, Util.getCountryCode(account.getNumber()),
        DELETION_REASON_TAG_NAME, deletionReason.tagValue)
        .increment();
  }

  private void delete(final Account account) {
    final CompletableFuture<Void> deleteStorageServiceDataFuture = secureStorageClient.deleteStoredData(
        account.getUuid());
    final CompletableFuture<Void> deleteBackupServiceDataFuture = secureBackupClient.deleteBackups(account.getUuid());
    final CompletableFuture<Void> deleteSecureValueRecoveryServiceDataFuture = secureValueRecovery2Client.deleteBackups(
        account.getUuid());

    final CompletableFuture<Void> deleteKeysFuture = CompletableFuture.allOf(
        keysManager.delete(account.getUuid()),
        keysManager.delete(account.getPhoneNumberIdentifier()));

    final CompletableFuture<Void> deleteMessagesFuture = CompletableFuture.allOf(
        messagesManager.clear(account.getUuid()),
        messagesManager.clear(account.getPhoneNumberIdentifier()));

    profilesManager.deleteAll(account.getUuid());
    registrationRecoveryPasswordsManager.removeForNumber(account.getNumber());

    deleteKeysFuture.join();
    deleteMessagesFuture.join();
    deleteStorageServiceDataFuture.join();
    deleteBackupServiceDataFuture.join();
    deleteSecureValueRecoveryServiceDataFuture.join();

    accounts.delete(account.getUuid());
    redisDelete(account);

    RedisOperation.unchecked(() ->
        account.getDevices().forEach(device ->
            clientPresenceManager.disconnectPresence(account.getUuid(), device.getId())));
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
        commands.setex(getAccountMapKey(account.getNumber()), CACHE_TTL_SECONDS, account.getUuid().toString());
        commands.setex(getAccountEntityKey(account.getUuid()), CACHE_TTL_SECONDS, accountJson);

        account.getUsernameHash().ifPresent(usernameHash ->
            commands.setex(getUsernameHashAccountMapKey(usernameHash), CACHE_TTL_SECONDS, account.getUuid().toString()));
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

        connection.async()
            .setex(getAccountMapKey(account.getNumber()), CACHE_TTL_SECONDS, account.getUuid().toString())
            .toCompletableFuture(),

        connection.async().setex(getAccountEntityKey(account.getUuid()), CACHE_TTL_SECONDS, accountJson)
            .toCompletableFuture(),

        account.getUsernameHash()
            .map(usernameHash -> connection.async()
                .setex(getUsernameHashAccountMapKey(usernameHash), CACHE_TTL_SECONDS, account.getUuid().toString())
                .toCompletableFuture())
            .orElseGet(() -> CompletableFuture.completedFuture(null))
    ));
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
            getAccountMapKey(account.getNumber()),
            getAccountMapKey(account.getPhoneNumberIdentifier().toString()),
            getAccountEntityKey(account.getUuid()));

        account.getUsernameHash().ifPresent(usernameHash -> connection.sync().del(getUsernameHashAccountMapKey(usernameHash)));
      });
    }
  }

  private CompletableFuture<Void> redisDeleteAsync(final Account account) {
    @SuppressWarnings("resource") final Timer.Context timerContext = redisDeleteTimer.time();

    final List<String> keysToDelete = new ArrayList<>(4);
    keysToDelete.add(getAccountMapKey(account.getNumber()));
    keysToDelete.add(getAccountMapKey(account.getPhoneNumberIdentifier().toString()));
    keysToDelete.add(getAccountEntityKey(account.getUuid()));

    account.getUsernameHash().ifPresent(usernameHash -> keysToDelete.add(getUsernameHashAccountMapKey(usernameHash)));

    return cacheCluster.withCluster(connection -> connection.async().del(keysToDelete.toArray(new String[0])))
        .toCompletableFuture()
        .thenRun(timerContext::close);
  }
}
