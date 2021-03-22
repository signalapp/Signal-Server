/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;


import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AmbiguousIdentifier;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;

public class AccountsManager {

  private static final MetricRegistry metricRegistry   = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          createTimer      = metricRegistry.timer(name(AccountsManager.class, "create"     ));
  private static final Timer          updateTimer      = metricRegistry.timer(name(AccountsManager.class, "update"     ));
  private static final Timer          getByNumberTimer = metricRegistry.timer(name(AccountsManager.class, "getByNumber"));
  private static final Timer          getByUuidTimer   = metricRegistry.timer(name(AccountsManager.class, "getByUuid"  ));
  private static final Timer          deleteTimer      = metricRegistry.timer(name(AccountsManager.class, "delete"));

  private static final Timer redisSetTimer       = metricRegistry.timer(name(AccountsManager.class, "redisSet"      ));
  private static final Timer redisNumberGetTimer = metricRegistry.timer(name(AccountsManager.class, "redisNumberGet"));
  private static final Timer redisUuidGetTimer   = metricRegistry.timer(name(AccountsManager.class, "redisUuidGet"  ));
  private static final Timer redisDeleteTimer    = metricRegistry.timer(name(AccountsManager.class, "redisDelete"   ));

  private static final String DELETE_COUNTER_NAME       = name(AccountsManager.class, "deleteCounter");
  private static final String DELETE_ERROR_COUNTER_NAME = name(AccountsManager.class, "deleteError");
  private static final String COUNTRY_CODE_TAG_NAME     = "country";
  private static final String DELETION_REASON_TAG_NAME  = "reason";


  private final Logger logger = LoggerFactory.getLogger(AccountsManager.class);

  private final Accounts                  accounts;
  private final FaultTolerantRedisCluster cacheCluster;
  private final DirectoryQueue            directoryQueue;
  private final KeysDynamoDb              keysDynamoDb;
  private final MessagesManager           messagesManager;
  private final UsernamesManager          usernamesManager;
  private final ProfilesManager           profilesManager;
  private final SecureStorageClient       secureStorageClient;
  private final ObjectMapper              mapper;

  public enum DeletionReason {
    ADMIN_DELETED("admin"),
    EXPIRED      ("expired"),
    USER_REQUEST ("userRequest");

    private final String tagValue;

    DeletionReason(final String tagValue) {
      this.tagValue = tagValue;
    }
  }

  public AccountsManager(Accounts accounts, FaultTolerantRedisCluster cacheCluster, final DirectoryQueue directoryQueue, final KeysDynamoDb keysDynamoDb, final MessagesManager messagesManager, final UsernamesManager usernamesManager, final ProfilesManager profilesManager, final SecureStorageClient secureStorageClient) {
    this.accounts            = accounts;
    this.cacheCluster        = cacheCluster;
    this.directoryQueue      = directoryQueue;
    this.keysDynamoDb        = keysDynamoDb;
    this.messagesManager     = messagesManager;
    this.usernamesManager    = usernamesManager;
    this.profilesManager     = profilesManager;
    this.secureStorageClient = secureStorageClient;
    this.mapper              = SystemMapper.getMapper();
  }

  public boolean create(Account account) {
    try (Timer.Context ignored = createTimer.time()) {
      boolean freshUser = databaseCreate(account);
      redisSet(account);

      return freshUser;
    }
  }

  public void update(Account account) {
    try (Timer.Context ignored = updateTimer.time()) {
      redisSet(account);
      databaseUpdate(account);
    }
  }

  public Optional<Account> get(AmbiguousIdentifier identifier) {
    if      (identifier.hasNumber()) return get(identifier.getNumber());
    else if (identifier.hasUuid())   return get(identifier.getUuid());
    else                             throw new AssertionError();
  }

  public Optional<Account> get(String number) {
    try (Timer.Context ignored = getByNumberTimer.time()) {
      Optional<Account> account = redisGet(number);

      if (!account.isPresent()) {
        account = databaseGet(number);
        account.ifPresent(value -> redisSet(value));
      }

      return account;
    }
  }

  public Optional<Account> get(UUID uuid) {
    try (Timer.Context ignored = getByUuidTimer.time()) {
      Optional<Account> account = redisGet(uuid);

      if (!account.isPresent()) {
        account = databaseGet(uuid);
        account.ifPresent(value -> redisSet(value));
      }

      return account;
    }
  }


  public List<Account> getAllFrom(int length) {
    return accounts.getAllFrom(length);
  }

  public List<Account> getAllFrom(UUID uuid, int length) {
    return accounts.getAllFrom(uuid, length);
  }

  public void delete(final Account account, final DeletionReason deletionReason) {
    try (final Timer.Context ignored = deleteTimer.time()) {
      final CompletableFuture<Void> deleteStorageServiceDataFuture = secureStorageClient.deleteStoredData(account.getUuid());

      usernamesManager.delete(account.getUuid());
      directoryQueue.deleteAccount(account);
      profilesManager.deleteAll(account.getUuid());
      keysDynamoDb.delete(account);
      messagesManager.clear(account.getUuid());

      deleteStorageServiceDataFuture.join();

      redisDelete(account);
      databaseDelete(account);
    } catch (final Exception e) {
      logger.warn("Failed to delete account", e);

      Metrics.counter(DELETE_ERROR_COUNTER_NAME,
          COUNTRY_CODE_TAG_NAME, Util.getCountryCode(account.getNumber()),
          DELETION_REASON_TAG_NAME, deletionReason.tagValue).increment();

      throw e;
    }

    Metrics.counter(DELETE_COUNTER_NAME,
                    COUNTRY_CODE_TAG_NAME,    Util.getCountryCode(account.getNumber()),
                    DELETION_REASON_TAG_NAME, deletionReason.tagValue)
           .increment();
  }

  private String getAccountMapKey(String number) {
    return "AccountMap::" + number;
  }

  private String getAccountEntityKey(UUID uuid) {
    return "Account3::" + uuid.toString();
  }

  private void redisSet(Account account) {
    try (Timer.Context ignored = redisSetTimer.time()) {
      final String accountJson = mapper.writeValueAsString(account);

      cacheCluster.useCluster(connection -> {
        final RedisAdvancedClusterCommands<String, String> commands = connection.sync();

        commands.set(getAccountMapKey(account.getNumber()), account.getUuid().toString());
        commands.set(getAccountEntityKey(account.getUuid()), accountJson);
      });
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  private Optional<Account> redisGet(String number) {
    try (Timer.Context ignored = redisNumberGetTimer.time()) {
      final String uuid = cacheCluster.withCluster(connection -> connection.sync().get(getAccountMapKey(number)));

      if (uuid != null) return redisGet(UUID.fromString(uuid));
      else              return Optional.empty();
    } catch (IllegalArgumentException e) {
      logger.warn("Deserialization error", e);
      return Optional.empty();
    } catch (RedisException e) {
      logger.warn("Redis failure", e);
      return Optional.empty();
    }
  }

  private Optional<Account> redisGet(UUID uuid) {
    try (Timer.Context ignored = redisUuidGetTimer.time()) {
      final String json = cacheCluster.withCluster(connection -> connection.sync().get(getAccountEntityKey(uuid)));

      if (json != null) {
        Account account = mapper.readValue(json, Account.class);
        account.setUuid(uuid);

        return Optional.of(account);
      }

      return Optional.empty();
    } catch (IOException e) {
      logger.warn("Deserialization error", e);
      return Optional.empty();
    } catch (RedisException e) {
      logger.warn("Redis failure", e);
      return Optional.empty();
    }
  }

  private void redisDelete(final Account account) {
    try (final Timer.Context ignored = redisDeleteTimer.time()) {
      cacheCluster.useCluster(connection -> connection.sync().del(getAccountMapKey(account.getNumber()), getAccountEntityKey(account.getUuid())));
    }
  }

  private Optional<Account> databaseGet(String number) {
    return accounts.get(number);
  }

  private Optional<Account> databaseGet(UUID uuid) {
    return accounts.get(uuid);
  }

  private boolean databaseCreate(Account account) {
    return accounts.create(account);
  }

  private void databaseUpdate(Account account) {
    accounts.update(account);
  }

  private void databaseDelete(final Account account) {
    accounts.delete(account.getUuid());
  }
}
