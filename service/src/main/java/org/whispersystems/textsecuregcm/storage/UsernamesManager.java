package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

public class UsernamesManager {

  private static final MetricRegistry metricRegistry        = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          createTimer           = metricRegistry.timer(name(AccountsManager.class, "create"          ));
  private static final Timer          deleteTimer           = metricRegistry.timer(name(AccountsManager.class, "delete"          ));
  private static final Timer          getByUuidTimer        = metricRegistry.timer(name(AccountsManager.class, "getByUuid"       ));
  private static final Timer          getByUsernameTimer    = metricRegistry.timer(name(AccountsManager.class, "getByUsername"   ));

  private static final Timer          redisSetTimer         = metricRegistry.timer(name(AccountsManager.class, "redisSet"        ));
  private static final Timer          redisUuidGetTimer     = metricRegistry.timer(name(AccountsManager.class, "redisUuidGet"    ));
  private static final Timer          redisUsernameGetTimer = metricRegistry.timer(name(AccountsManager.class, "redisUsernameGet"));

  private final Logger logger = LoggerFactory.getLogger(AccountsManager.class);

  private final Usernames                 usernames;
  private final ReservedUsernames         reservedUsernames;
  private final ReplicatedJedisPool       cacheClient;
  private final FaultTolerantRedisCluster cacheCluster;

  public UsernamesManager(Usernames usernames, ReservedUsernames reservedUsernames, ReplicatedJedisPool cacheClient, FaultTolerantRedisCluster cacheCluster) {
    this.usernames         = usernames;
    this.reservedUsernames = reservedUsernames;
    this.cacheClient       = cacheClient;
    this.cacheCluster      = cacheCluster;
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
    try (Jedis         jedis   = cacheClient.getWriteResource();
         Timer.Context ignored = redisSetTimer.time())
    {
      final String uuidMapKey = getUuidMapKey(uuid);
      final String usernameMapKey = getUsernameMapKey(username);

      Optional.ofNullable(jedis.get(uuidMapKey)).ifPresent(oldUsername -> jedis.del(getUsernameMapKey(oldUsername)));

      jedis.set(uuidMapKey, username);
      jedis.set(usernameMapKey, uuid.toString());

      cacheCluster.useWriteCluster(connection -> {
        final RedisAdvancedClusterAsyncCommands<String, String> asyncCommands = connection.async();

        asyncCommands.get(uuidMapKey).thenAccept(oldUsername -> {
          if (oldUsername != null) {
            asyncCommands.del(getUsernameMapKey(oldUsername));
          }

          asyncCommands.set(uuidMapKey, username);
          asyncCommands.set(usernameMapKey, uuid.toString());
        });
      });
    } catch (JedisException e) {
      if (required) throw e;
      else          logger.warn("Ignoring jedis failure", e);
    }
  }

  private Optional<UUID> redisGet(String username) {
    try (Jedis         jedis   = cacheClient.getReadResource();
         Timer.Context ignored = redisUsernameGetTimer.time())
    {
      String result = jedis.get(getUsernameMapKey(username));

      if (result == null) return Optional.empty();
      else                return Optional.of(UUID.fromString(result));
    } catch (JedisException e) {
      logger.warn("Redis get failure", e);
      return Optional.empty();
    }
  }

  private Optional<String> redisGet(UUID uuid) {
    try (Jedis         jedis   = cacheClient.getReadResource();
         Timer.Context ignored = redisUuidGetTimer.time())
    {
      return Optional.ofNullable(jedis.get(getUuidMapKey(uuid)));
    } catch (JedisException e) {
      logger.warn("Redis get failure", e);
      return Optional.empty();
    }
  }

  private void redisDelete(UUID uuid) {
    try (Jedis         jedis   = cacheClient.getWriteResource();
         Timer.Context ignored = redisUuidGetTimer.time())
    {
      final String uuidMapKey = getUuidMapKey(uuid);

      redisGet(uuid).ifPresent(username -> {
        jedis.del(getUsernameMapKey(username));
        jedis.del(uuidMapKey);
      });

      cacheCluster.useWriteCluster(connection -> {
        final RedisAdvancedClusterAsyncCommands<String, String> asyncCommands = connection.async();

        asyncCommands.get(uuidMapKey).thenAccept(username -> {
          if (username != null) {
            asyncCommands.del(getUsernameMapKey(username));
            asyncCommands.del(uuidMapKey);
          }
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
