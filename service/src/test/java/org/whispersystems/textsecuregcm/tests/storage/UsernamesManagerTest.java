package org.whispersystems.textsecuregcm.tests.storage;

import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.Test;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.storage.ReservedUsernames;
import org.whispersystems.textsecuregcm.storage.Usernames;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;
import redis.clients.jedis.Jedis;

import java.util.Optional;
import java.util.UUID;

import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class UsernamesManagerTest {

  @Test
  public void testGetByUsernameInCache() {
    RedisAdvancedClusterCommands<String, String> commands = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster cacheCluster                = RedisClusterHelper.buildMockRedisCluster(commands);
    Usernames                 usernames                   = mock(Usernames.class);
    ReservedUsernames         reserved                    = mock(ReservedUsernames.class);

    ReplicatedJedisPool cacheClient  = mock(ReplicatedJedisPool.class);
    when(cacheClient.getWriteResource()).thenReturn(mock(Jedis.class));

    UUID uuid = UUID.randomUUID();

    when(commands.get(eq("UsernameByUsername::n00bkiller"))).thenReturn(uuid.toString());

    UsernamesManager usernamesManager = new UsernamesManager(usernames, reserved, cacheClient, cacheCluster);
    Optional<UUID>   retrieved        = usernamesManager.get("n00bkiller");

    assertTrue(retrieved.isPresent());
    assertEquals(retrieved.get(), uuid);

    verify(commands, times(1)).get(eq("UsernameByUsername::n00bkiller"));
    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(usernames);
  }

  @Test
  public void testGetByUuidInCache() {
    RedisAdvancedClusterCommands<String, String> commands = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster cacheCluster                = RedisClusterHelper.buildMockRedisCluster(commands);
    Usernames                 usernames                   = mock(Usernames.class);
    ReservedUsernames         reserved                    = mock(ReservedUsernames.class);

    ReplicatedJedisPool cacheClient  = mock(ReplicatedJedisPool.class);
    when(cacheClient.getWriteResource()).thenReturn(mock(Jedis.class));

    UUID uuid = UUID.randomUUID();

    when(commands.get(eq("UsernameByUuid::" + uuid.toString()))).thenReturn("n00bkiller");

    UsernamesManager usernamesManager = new UsernamesManager(usernames, reserved, cacheClient, cacheCluster);
    Optional<String> retrieved        = usernamesManager.get(uuid);

    assertTrue(retrieved.isPresent());
    assertEquals(retrieved.get(), "n00bkiller");

    verify(commands, times(1)).get(eq("UsernameByUuid::" + uuid.toString()));
    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(usernames);
  }


  @Test
  public void testGetByUsernameNotInCache() {
    RedisAdvancedClusterCommands<String, String> commands = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster cacheCluster                = RedisClusterHelper.buildMockRedisCluster(commands);
    Usernames                 usernames                   = mock(Usernames.class);
    ReservedUsernames         reserved                    = mock(ReservedUsernames.class);

    ReplicatedJedisPool cacheClient  = mock(ReplicatedJedisPool.class);
    when(cacheClient.getWriteResource()).thenReturn(mock(Jedis.class));

    UUID uuid = UUID.randomUUID();

    when(commands.get(eq("UsernameByUsername::n00bkiller"))).thenReturn(null);
    when(usernames.get(eq("n00bkiller"))).thenReturn(Optional.of(uuid));

    UsernamesManager usernamesManager = new UsernamesManager(usernames, reserved, cacheClient, cacheCluster);
    Optional<UUID>   retrieved        = usernamesManager.get("n00bkiller");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), uuid);

    verify(commands, times(1)).get(eq("UsernameByUsername::n00bkiller"));
    verify(commands, times(1)).set(eq("UsernameByUsername::n00bkiller"), eq(uuid.toString()));
    verify(commands, times(1)).set(eq("UsernameByUuid::" + uuid.toString()), eq("n00bkiller"));
    verify(commands, times(1)).get(eq("UsernameByUuid::" + uuid.toString()));
    verifyNoMoreInteractions(commands);

    verify(usernames, times(1)).get(eq("n00bkiller"));
    verifyNoMoreInteractions(usernames);
  }

  @Test
  public void testGetByUuidNotInCache() {
    RedisAdvancedClusterCommands<String, String> commands = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster cacheCluster                = RedisClusterHelper.buildMockRedisCluster(commands);
    Usernames                 usernames                   = mock(Usernames.class);
    ReservedUsernames         reserved                    = mock(ReservedUsernames.class);

    ReplicatedJedisPool cacheClient  = mock(ReplicatedJedisPool.class);
    when(cacheClient.getWriteResource()).thenReturn(mock(Jedis.class));

    UUID uuid = UUID.randomUUID();

    when(commands.get(eq("UsernameByUuid::" + uuid.toString()))).thenReturn(null);
    when(usernames.get(eq(uuid))).thenReturn(Optional.of("n00bkiller"));

    UsernamesManager usernamesManager = new UsernamesManager(usernames, reserved, cacheClient, cacheCluster);
    Optional<String> retrieved        = usernamesManager.get(uuid);

    assertTrue(retrieved.isPresent());
    assertEquals(retrieved.get(), "n00bkiller");

    verify(commands, times(2)).get(eq("UsernameByUuid::" + uuid));
    verify(commands, times(1)).set(eq("UsernameByUuid::" + uuid), eq("n00bkiller"));
    verify(commands, times(1)).set(eq("UsernameByUsername::n00bkiller"), eq(uuid.toString()));
    verifyNoMoreInteractions(commands);

    verify(usernames, times(1)).get(eq(uuid));
    verifyNoMoreInteractions(usernames);
  }

  @Test
  public void testGetByUsernameBrokenCache() {
    RedisAdvancedClusterCommands<String, String> commands = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster cacheCluster                = RedisClusterHelper.buildMockRedisCluster(commands);
    Usernames                 usernames                   = mock(Usernames.class);
    ReservedUsernames         reserved                    = mock(ReservedUsernames.class);

    ReplicatedJedisPool cacheClient  = mock(ReplicatedJedisPool.class);
    when(cacheClient.getWriteResource()).thenReturn(mock(Jedis.class));

    UUID                uuid        = UUID.randomUUID();

    when(commands.get(eq("UsernameByUsername::n00bkiller"))).thenThrow(new RedisException("Connection lost!"));
    when(usernames.get(eq("n00bkiller"))).thenReturn(Optional.of(uuid));

    UsernamesManager usernamesManager = new UsernamesManager(usernames, reserved, cacheClient, cacheCluster);
    Optional<UUID>   retrieved        = usernamesManager.get("n00bkiller");

    assertTrue(retrieved.isPresent());
    assertEquals(retrieved.get(), uuid);

    verify(commands, times(1)).get(eq("UsernameByUsername::n00bkiller"));
    verify(commands, times(1)).set(eq("UsernameByUsername::n00bkiller"), eq(uuid.toString()));
    verify(commands, times(1)).set(eq("UsernameByUuid::" + uuid.toString()), eq("n00bkiller"));
    verify(commands, times(1)).get(eq("UsernameByUuid::" + uuid.toString()));
    verifyNoMoreInteractions(commands);

    verify(usernames, times(1)).get(eq("n00bkiller"));
    verifyNoMoreInteractions(usernames);
  }

  @Test
  public void testGetAccountByUuidBrokenCache() {
    RedisAdvancedClusterCommands<String, String> commands = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster cacheCluster                = RedisClusterHelper.buildMockRedisCluster(commands);
    Usernames                 usernames                   = mock(Usernames.class);
    ReservedUsernames         reserved                    = mock(ReservedUsernames.class);

    ReplicatedJedisPool cacheClient  = mock(ReplicatedJedisPool.class);
    when(cacheClient.getWriteResource()).thenReturn(mock(Jedis.class));

    UUID uuid = UUID.randomUUID();

    when(commands.get(eq("UsernameByUuid::" + uuid))).thenThrow(new RedisException("Connection lost!"));
    when(usernames.get(eq(uuid))).thenReturn(Optional.of("n00bkiller"));

    UsernamesManager usernamesManager = new UsernamesManager(usernames, reserved, cacheClient, cacheCluster);
    Optional<String>   retrieved        = usernamesManager.get(uuid);

    assertTrue(retrieved.isPresent());
    assertEquals(retrieved.get(), "n00bkiller");

    verify(commands, times(2)).get(eq("UsernameByUuid::" + uuid));
    verifyNoMoreInteractions(commands);

    verify(usernames, times(1)).get(eq(uuid));
    verifyNoMoreInteractions(usernames);
  }

}
