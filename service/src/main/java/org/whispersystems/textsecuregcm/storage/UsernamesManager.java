/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class UsernamesManager {

  private static final MetricRegistry metricRegistry        = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          createTimer           = metricRegistry.timer(name(UsernamesManager.class, "create"          ));
  private static final Timer          deleteTimer           = metricRegistry.timer(name(UsernamesManager.class, "delete"          ));
  private static final Timer          getByUuidTimer        = metricRegistry.timer(name(UsernamesManager.class, "getByUuid"       ));
  private static final Timer          getByUsernameTimer    = metricRegistry.timer(name(UsernamesManager.class, "getByUsername"   ));

  private static final Timer          redisSetTimer         = metricRegistry.timer(name(UsernamesManager.class, "redisSet"        ));
  private static final Timer          redisUuidGetTimer     = metricRegistry.timer(name(UsernamesManager.class, "redisUuidGet"    ));
  private static final Timer          redisUsernameGetTimer = metricRegistry.timer(name(UsernamesManager.class, "redisUsernameGet"));

  private final Logger logger = LoggerFactory.getLogger(UsernamesManager.class);

  private final Usernames                 usernames;
  private final ReservedUsernames         reservedUsernames;
  private final FaultTolerantRedisCluster cacheCluster;

  public UsernamesManager(Usernames usernames, ReservedUsernames reservedUsernames, FaultTolerantRedisCluster cacheCluster) {
    this.usernames              = usernames;
    this.reservedUsernames      = reservedUsernames;
    this.cacheCluster           = cacheCluster;
  }

  public boolean put(UUID uuid, String username) {
    try (Timer.Context ignored = createTimer.time()) {
      if (reservedUsernames.isReserved(username, uuid)) {
        return false;
      }

      if (databasePut(uuid, username)) {
        redisSet(uuid, username, true);

        return true;
      }

      return false;
    }
  }

  public Optional<UUID> get(String username) {
    try (Timer.Context ignored = getByUsernameTimer.time()) {
      Optional<UUID> uuid = redisGet(username);

      if (uuid.isPresent()) {
        return uuid;
      }

      Optional<UUID> retrieved = databaseGet(username);
      retrieved.ifPresent(retrievedUuid -> redisSet(retrievedUuid, username, false));

      return retrieved;
    }
  }

  public Optional<String> get(UUID uuid) {
    try (Timer.Context ignored = getByUuidTimer.time()) {
      Optional<String> username = redisGet(uuid);

      if (username.isPresent()) {
        return username;
      }

      Optional<String> retrieved = databaseGet(uuid);
      retrieved.ifPresent(retrievedUsername -> redisSet(uuid, retrievedUsername, false));

      return retrieved;
    }
  }

  public void delete(UUID uuid) {
    try (Timer.Context ignored = deleteTimer.time()) {
      redisDelete(uuid);
      databaseDelete(uuid);
    }
  }

  private boolean databasePut(UUID uuid, String username) {
    return usernames.put(uuid, username);
  }

  private Optional<UUID> databaseGet(String username) {
    return usernames.get(username);
  }

  private void databaseDelete(UUID uuid) {
    usernames.delete(uuid);
  }

  private Optional<String> databaseGet(UUID uuid) {
    return usernames.get(uuid);
  }

  private void redisSet(UUID uuid, String username, boolean required) {
    final String uuidMapKey = getUuidMapKey(uuid);
    final String usernameMapKey = getUsernameMapKey(username);

    try (Timer.Context ignored = redisSetTimer.time()) {
      cacheCluster.useCluster(connection -> {
        final RedisAdvancedClusterCommands<String, String> commands = connection.sync();

        final Optional<String> maybeOldUsername = Optional.ofNullable(commands.get(uuidMapKey));

        maybeOldUsername.ifPresent(oldUsername -> commands.del(getUsernameMapKey(oldUsername)));
        commands.set(uuidMapKey, username);
        commands.set(usernameMapKey, uuid.toString());
      });
    } catch (RedisException e) {
      if (required) throw e;
      else          logger.warn("Ignoring Redis failure", e);
    }
  }

  private Optional<UUID> redisGet(String username) {
    try (Timer.Context ignored = redisUsernameGetTimer.time()) {
      final String result = cacheCluster.withCluster(connection -> connection.sync().get(getUsernameMapKey(username)));

      if (result == null) return Optional.empty();
      else                return Optional.of(UUID.fromString(result));
    } catch (RedisException e) {
      logger.warn("Redis get failure", e);
      return Optional.empty();
    }
  }

  private Optional<String> redisGet(UUID uuid) {
    try (Timer.Context ignored = redisUuidGetTimer.time()) {
      final String result = cacheCluster.withCluster(connection -> connection.sync().get(getUuidMapKey(uuid)));

      return Optional.ofNullable(result);
    } catch (RedisException e) {
      logger.warn("Redis get failure", e);
      return Optional.empty();
    }
  }

  private void redisDelete(UUID uuid) {
    try (Timer.Context ignored = redisUuidGetTimer.time()) {
      cacheCluster.useCluster(connection -> {
        final RedisAdvancedClusterCommands<String, String> commands = connection.sync();

        commands.del(getUuidMapKey(uuid));

        redisGet(uuid).ifPresent(username -> {
          commands.del(getUsernameMapKey(username));
        });
      });
    }
  }

  private String getUuidMapKey(UUID uuid) {
    return "UsernameByUuid::" + uuid.toString();
  }

  private String getUsernameMapKey(String username) {
    return "UsernameByUsername::" + username;
  }

}
