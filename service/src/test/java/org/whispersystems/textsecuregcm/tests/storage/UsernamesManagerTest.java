package org.whispersystems.textsecuregcm.tests.storage;

import org.junit.Test;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.storage.ReservedUsernames;
import org.whispersystems.textsecuregcm.storage.Usernames;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;

import java.util.Optional;
import java.util.UUID;

import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

public class UsernamesManagerTest {

  @Test
  public void testGetByUsernameInCache() {
    ReplicatedJedisPool cacheClient = mock(ReplicatedJedisPool.class);
    Jedis               jedis       = mock(Jedis.class              );
    Usernames           usernames   = mock(Usernames.class          );
    ReservedUsernames   reserved    = mock(ReservedUsernames.class);

    UUID uuid = UUID.randomUUID();

    when(cacheClient.getReadResource()).thenReturn(jedis);
    when(jedis.get(eq("UsernameByUsername::n00bkiller"))).thenReturn(uuid.toString());

    UsernamesManager usernamesManager = new UsernamesManager(usernames, reserved, cacheClient);
    Optional<UUID>   retrieved        = usernamesManager.get("n00bkiller");

    assertTrue(retrieved.isPresent());
    assertEquals(retrieved.get(), uuid);

    verify(jedis, times(1)).get(eq("UsernameByUsername::n00bkiller"));
    verify(jedis, times(1)).close();
    verifyNoMoreInteractions(jedis);
    verifyNoMoreInteractions(usernames);
  }

  @Test
  public void testGetByUuidInCache() {
    ReplicatedJedisPool cacheClient = mock(ReplicatedJedisPool.class);
    Jedis               jedis       = mock(Jedis.class              );
    Usernames           usernames   = mock(Usernames.class          );
    ReservedUsernames   reserved    = mock(ReservedUsernames.class);

    UUID uuid = UUID.randomUUID();

    when(cacheClient.getReadResource()).thenReturn(jedis);
    when(jedis.get(eq("UsernameByUuid::" + uuid.toString()))).thenReturn("n00bkiller");

    UsernamesManager usernamesManager = new UsernamesManager(usernames, reserved, cacheClient);
    Optional<String> retrieved        = usernamesManager.get(uuid);

    assertTrue(retrieved.isPresent());
    assertEquals(retrieved.get(), "n00bkiller");

    verify(jedis, times(1)).get(eq("UsernameByUuid::" + uuid.toString()));
    verify(jedis, times(1)).close();
    verifyNoMoreInteractions(jedis);
    verifyNoMoreInteractions(usernames);
  }


  @Test
  public void testGetByUsernameNotInCache() {
    ReplicatedJedisPool cacheClient = mock(ReplicatedJedisPool.class);
    Jedis               jedis       = mock(Jedis.class              );
    Usernames           usernames   = mock(Usernames.class          );
    ReservedUsernames   reserved    = mock(ReservedUsernames.class);

    UUID                uuid             = UUID.randomUUID();


    when(cacheClient.getReadResource()).thenReturn(jedis);
    when(cacheClient.getWriteResource()).thenReturn(jedis);
    when(jedis.get(eq("UsernameByUsername::n00bkiller"))).thenReturn(null);
    when(usernames.get(eq("n00bkiller"))).thenReturn(Optional.of(uuid));

    UsernamesManager usernamesManager = new UsernamesManager(usernames, reserved, cacheClient);
    Optional<UUID>   retrieved        = usernamesManager.get("n00bkiller");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), uuid);

    verify(jedis, times(1)).get(eq("UsernameByUsername::n00bkiller"));
    verify(jedis, times(1)).set(eq("UsernameByUsername::n00bkiller"), eq(uuid.toString()));
    verify(jedis, times(1)).set(eq("UsernameByUuid::" + uuid.toString()), eq("n00bkiller"));
    verify(jedis, times(1)).get(eq("UsernameByUuid::" + uuid.toString()));
    verify(jedis, times(2)).close();
    verifyNoMoreInteractions(jedis);

    verify(usernames, times(1)).get(eq("n00bkiller"));
    verifyNoMoreInteractions(usernames);
  }

  @Test
  public void testGetByUuidNotInCache() {
    ReplicatedJedisPool cacheClient = mock(ReplicatedJedisPool.class);
    Jedis               jedis       = mock(Jedis.class              );
    Usernames           usernames   = mock(Usernames.class          );
    ReservedUsernames   reserved    = mock(ReservedUsernames.class);

    UUID uuid = UUID.randomUUID();

    when(cacheClient.getReadResource()).thenReturn(jedis);
    when(cacheClient.getWriteResource()).thenReturn(jedis);
    when(jedis.get(eq("UsernameByUuid::" + uuid.toString()))).thenReturn(null);
    when(usernames.get(eq(uuid))).thenReturn(Optional.of("n00bkiller"));

    UsernamesManager usernamesManager = new UsernamesManager(usernames, reserved, cacheClient);
    Optional<String> retrieved        = usernamesManager.get(uuid);

    assertTrue(retrieved.isPresent());
    assertEquals(retrieved.get(), "n00bkiller");

    verify(jedis, times(2)).get(eq("UsernameByUuid::" + uuid));
    verify(jedis, times(1)).set(eq("UsernameByUuid::" + uuid), eq("n00bkiller"));
    verify(jedis, times(1)).set(eq("UsernameByUsername::n00bkiller"), eq(uuid.toString()));
    verify(jedis, times(2)).close();
    verifyNoMoreInteractions(jedis);

    verify(usernames, times(1)).get(eq(uuid));
    verifyNoMoreInteractions(usernames);
  }

  @Test
  public void testGetByUsernameBrokenCache() {
    ReplicatedJedisPool cacheClient = mock(ReplicatedJedisPool.class);
    Jedis               jedis       = mock(Jedis.class              );
    Usernames           usernames   = mock(Usernames.class          );
    ReservedUsernames   reserved    = mock(ReservedUsernames.class);

    UUID                uuid        = UUID.randomUUID();

    when(cacheClient.getReadResource()).thenReturn(jedis);
    when(cacheClient.getWriteResource()).thenReturn(jedis);
    when(jedis.get(eq("UsernameByUsername::n00bkiller"))).thenThrow(new JedisException("Connection lost!"));
    when(usernames.get(eq("n00bkiller"))).thenReturn(Optional.of(uuid));

    UsernamesManager usernamesManager = new UsernamesManager(usernames, reserved, cacheClient);
    Optional<UUID>   retrieved        = usernamesManager.get("n00bkiller");

    assertTrue(retrieved.isPresent());
    assertEquals(retrieved.get(), uuid);

    verify(jedis, times(1)).get(eq("UsernameByUsername::n00bkiller"));
    verify(jedis, times(1)).set(eq("UsernameByUsername::n00bkiller"), eq(uuid.toString()));
    verify(jedis, times(1)).set(eq("UsernameByUuid::" + uuid.toString()), eq("n00bkiller"));
    verify(jedis, times(1)).get(eq("UsernameByUuid::" + uuid.toString()));
    verify(jedis, times(2)).close();
    verifyNoMoreInteractions(jedis);

    verify(usernames, times(1)).get(eq("n00bkiller"));
    verifyNoMoreInteractions(usernames);
  }

  @Test
  public void testGetAccountByUuidBrokenCache() {
    ReplicatedJedisPool cacheClient = mock(ReplicatedJedisPool.class);
    Jedis               jedis       = mock(Jedis.class              );
    Usernames           usernames   = mock(Usernames.class          );
    ReservedUsernames   reserved    = mock(ReservedUsernames.class);

    UUID                uuid        = UUID.randomUUID();

    when(cacheClient.getReadResource()).thenReturn(jedis);
    when(cacheClient.getWriteResource()).thenReturn(jedis);
    when(jedis.get(eq("UsernameByUuid::" + uuid))).thenThrow(new JedisException("Connection lost!"));
    when(usernames.get(eq(uuid))).thenReturn(Optional.of("n00bkiller"));

    UsernamesManager usernamesManager = new UsernamesManager(usernames, reserved, cacheClient);
    Optional<String>   retrieved        = usernamesManager.get(uuid);

    assertTrue(retrieved.isPresent());
    assertEquals(retrieved.get(), "n00bkiller");

    verify(jedis, times(2)).get(eq("UsernameByUuid::" + uuid));
    verify(jedis, times(2)).close();
    verifyNoMoreInteractions(jedis);

    verify(usernames, times(1)).get(eq(uuid));
    verifyNoMoreInteractions(usernames);
  }

}
