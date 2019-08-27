package org.whispersystems.textsecuregcm.tests.storage;

import org.junit.Test;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;

import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;

import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

public class AccountsManagerTest {

  @Test
  public void testGetAccountByNumberInCache() {
    ReplicatedJedisPool cacheClient      = mock(ReplicatedJedisPool.class);
    Jedis               jedis            = mock(Jedis.class              );
    Accounts            accounts         = mock(Accounts.class           );
    DirectoryManager    directoryManager = mock(DirectoryManager.class   );

    UUID uuid = UUID.randomUUID();

    when(cacheClient.getReadResource()).thenReturn(jedis);
    when(jedis.get(eq("AccountMap::+14152222222"))).thenReturn(uuid.toString());
    when(jedis.get(eq("Account3::" + uuid.toString()))).thenReturn("{\"number\": \"+14152222222\", \"name\": \"test\"}");

    AccountsManager   accountsManager = new AccountsManager(accounts, directoryManager, cacheClient);
    Optional<Account> account         = accountsManager.get("+14152222222");

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(account.get().getProfileName(), "test");

    verify(jedis, times(1)).get(eq("AccountMap::+14152222222"));
    verify(jedis, times(1)).get(eq("Account3::" + uuid.toString()));
    verify(jedis, times(1)).close();
    verifyNoMoreInteractions(jedis);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  public void testGetAccountByUuidInCache() {
    ReplicatedJedisPool cacheClient      = mock(ReplicatedJedisPool.class);
    Jedis               jedis            = mock(Jedis.class              );
    Accounts            accounts         = mock(Accounts.class           );
    DirectoryManager    directoryManager = mock(DirectoryManager.class   );

    UUID uuid = UUID.randomUUID();

    when(cacheClient.getReadResource()).thenReturn(jedis);
    when(jedis.get(eq("Account3::" + uuid.toString()))).thenReturn("{\"number\": \"+14152222222\", \"name\": \"test\"}");

    AccountsManager   accountsManager = new AccountsManager(accounts, directoryManager, cacheClient);
    Optional<Account> account         = accountsManager.get(uuid);

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(account.get().getUuid(), uuid);
    assertEquals(account.get().getProfileName(), "test");

    verify(jedis, times(1)).get(eq("Account3::" + uuid.toString()));
    verify(jedis, times(1)).close();
    verifyNoMoreInteractions(jedis);
    verifyNoMoreInteractions(accounts);
  }


  @Test
  public void testGetAccountByNumberNotInCache() {
    ReplicatedJedisPool cacheClient      = mock(ReplicatedJedisPool.class);
    Jedis               jedis            = mock(Jedis.class              );
    Accounts            accounts         = mock(Accounts.class           );
    DirectoryManager    directoryManager = mock(DirectoryManager.class   );
    UUID                uuid             = UUID.randomUUID();
    Account             account          = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    when(cacheClient.getReadResource()).thenReturn(jedis);
    when(cacheClient.getWriteResource()).thenReturn(jedis);
    when(jedis.get(eq("AccountMap::+14152222222"))).thenReturn(null);
    when(accounts.get(eq("+14152222222"))).thenReturn(Optional.of(account));

    AccountsManager   accountsManager = new AccountsManager(accounts, directoryManager, cacheClient);
    Optional<Account> retrieved       = accountsManager.get("+14152222222");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(jedis, times(1)).get(eq("AccountMap::+14152222222"));
    verify(jedis, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(jedis, times(1)).set(eq("Account3::" + uuid.toString()), anyString());
    verify(jedis, times(2)).close();
    verifyNoMoreInteractions(jedis);

    verify(accounts, times(1)).get(eq("+14152222222"));
    verifyNoMoreInteractions(accounts);
  }

  @Test
  public void testGetAccountByUuidNotInCache() {
    ReplicatedJedisPool cacheClient      = mock(ReplicatedJedisPool.class);
    Jedis               jedis            = mock(Jedis.class              );
    Accounts            accounts         = mock(Accounts.class           );
    DirectoryManager    directoryManager = mock(DirectoryManager.class   );
    UUID                uuid             = UUID.randomUUID();
    Account             account          = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    when(cacheClient.getReadResource()).thenReturn(jedis);
    when(cacheClient.getWriteResource()).thenReturn(jedis);
    when(jedis.get(eq("Account3::" + uuid))).thenReturn(null);
    when(accounts.get(eq(uuid))).thenReturn(Optional.of(account));

    AccountsManager   accountsManager = new AccountsManager(accounts, directoryManager, cacheClient);
    Optional<Account> retrieved       = accountsManager.get(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(jedis, times(1)).get(eq("Account3::" + uuid));
    verify(jedis, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(jedis, times(1)).set(eq("Account3::" + uuid.toString()), anyString());
    verify(jedis, times(2)).close();
    verifyNoMoreInteractions(jedis);

    verify(accounts, times(1)).get(eq(uuid));
    verifyNoMoreInteractions(accounts);
  }

  @Test
  public void testGetAccountByNumberBrokenCache() {
    ReplicatedJedisPool cacheClient      = mock(ReplicatedJedisPool.class);
    Jedis               jedis            = mock(Jedis.class              );
    Accounts            accounts         = mock(Accounts.class           );
    DirectoryManager    directoryManager = mock(DirectoryManager.class   );
    UUID                uuid             = UUID.randomUUID();
    Account             account          = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    when(cacheClient.getReadResource()).thenReturn(jedis);
    when(cacheClient.getWriteResource()).thenReturn(jedis);
    when(jedis.get(eq("AccountMap::+14152222222"))).thenThrow(new JedisException("Connection lost!"));
    when(accounts.get(eq("+14152222222"))).thenReturn(Optional.of(account));

    AccountsManager   accountsManager = new AccountsManager(accounts, directoryManager, cacheClient);
    Optional<Account> retrieved       = accountsManager.get("+14152222222");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(jedis, times(1)).get(eq("AccountMap::+14152222222"));
    verify(jedis, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(jedis, times(1)).set(eq("Account3::" + uuid.toString()), anyString());
    verify(jedis, times(2)).close();
    verifyNoMoreInteractions(jedis);

    verify(accounts, times(1)).get(eq("+14152222222"));
    verifyNoMoreInteractions(accounts);
  }

  @Test
  public void testGetAccountByUuidBrokenCache() {
    ReplicatedJedisPool cacheClient      = mock(ReplicatedJedisPool.class);
    Jedis               jedis            = mock(Jedis.class              );
    Accounts            accounts         = mock(Accounts.class           );
    DirectoryManager    directoryManager = mock(DirectoryManager.class   );
    UUID                uuid             = UUID.randomUUID();
    Account             account          = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    when(cacheClient.getReadResource()).thenReturn(jedis);
    when(cacheClient.getWriteResource()).thenReturn(jedis);
    when(jedis.get(eq("Account3::" + uuid))).thenThrow(new JedisException("Connection lost!"));
    when(accounts.get(eq(uuid))).thenReturn(Optional.of(account));

    AccountsManager   accountsManager = new AccountsManager(accounts, directoryManager, cacheClient);
    Optional<Account> retrieved       = accountsManager.get(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(jedis, times(1)).get(eq("Account3::" + uuid));
    verify(jedis, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(jedis, times(1)).set(eq("Account3::" + uuid.toString()), anyString());
    verify(jedis, times(2)).close();
    verifyNoMoreInteractions(jedis);

    verify(accounts, times(1)).get(eq(uuid));
    verifyNoMoreInteractions(accounts);
  }


}
