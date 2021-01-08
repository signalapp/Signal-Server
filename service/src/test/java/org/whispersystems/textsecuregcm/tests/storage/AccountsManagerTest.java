/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.Test;
import org.whispersystems.textsecuregcm.entities.Profile;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DirectoryManager;
import org.whispersystems.textsecuregcm.storage.Keys;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;

import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;

import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AccountsManagerTest {

  @Test
  public void testGetAccountByNumberInCache() {
    RedisAdvancedClusterCommands<String, String> commands         = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster     = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts         = mock(Accounts.class);
    DirectoryManager                             directoryManager = mock(DirectoryManager.class);
    DirectoryQueue                               directoryQueue   = mock(DirectoryQueue.class);
    Keys                                         keys             = mock(Keys.class);
    MessagesManager                              messagesManager  = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager  = mock(ProfilesManager.class);

    UUID uuid = UUID.randomUUID();

    when(commands.get(eq("AccountMap::+14152222222"))).thenReturn(uuid.toString());
    when(commands.get(eq("Account3::" + uuid.toString()))).thenReturn("{\"number\": \"+14152222222\", \"name\": \"test\"}");

    AccountsManager   accountsManager = new AccountsManager(accounts, directoryManager, cacheCluster, directoryQueue, keys, messagesManager, usernamesManager, profilesManager);
    Optional<Account> account         = accountsManager.get("+14152222222");

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(account.get().getProfileName(), "test");

    verify(commands, times(1)).get(eq("AccountMap::+14152222222"));
    verify(commands, times(1)).get(eq("Account3::" + uuid.toString()));
    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(accounts);
  }

  @Test
  public void testGetAccountByUuidInCache() {
    RedisAdvancedClusterCommands<String, String> commands         = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster     = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts         = mock(Accounts.class);
    DirectoryManager                             directoryManager = mock(DirectoryManager.class);
    DirectoryQueue                               directoryQueue   = mock(DirectoryQueue.class);
    Keys                                         keys             = mock(Keys.class);
    MessagesManager                              messagesManager  = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager  = mock(ProfilesManager.class);

    UUID uuid = UUID.randomUUID();

    when(commands.get(eq("Account3::" + uuid.toString()))).thenReturn("{\"number\": \"+14152222222\", \"name\": \"test\"}");

    AccountsManager   accountsManager = new AccountsManager(accounts, directoryManager, cacheCluster, directoryQueue, keys, messagesManager, usernamesManager, profilesManager);
    Optional<Account> account         = accountsManager.get(uuid);

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(account.get().getUuid(), uuid);
    assertEquals(account.get().getProfileName(), "test");

    verify(commands, times(1)).get(eq("Account3::" + uuid.toString()));
    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(accounts);
  }


  @Test
  public void testGetAccountByNumberNotInCache() {
    RedisAdvancedClusterCommands<String, String> commands         = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster     = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts         = mock(Accounts.class);
    DirectoryManager                             directoryManager = mock(DirectoryManager.class);
    DirectoryQueue                               directoryQueue   = mock(DirectoryQueue.class);
    Keys                                         keys             = mock(Keys.class);
    MessagesManager                              messagesManager  = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager  = mock(ProfilesManager.class);
    UUID                                         uuid             = UUID.randomUUID();
    Account                                      account          = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    when(commands.get(eq("AccountMap::+14152222222"))).thenReturn(null);
    when(accounts.get(eq("+14152222222"))).thenReturn(Optional.of(account));

    AccountsManager   accountsManager = new AccountsManager(accounts, directoryManager, cacheCluster, directoryQueue, keys, messagesManager, usernamesManager, profilesManager);
    Optional<Account> retrieved       = accountsManager.get("+14152222222");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("AccountMap::+14152222222"));
    verify(commands, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(commands, times(1)).set(eq("Account3::" + uuid.toString()), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).get(eq("+14152222222"));
    verifyNoMoreInteractions(accounts);
  }

  @Test
  public void testGetAccountByUuidNotInCache() {
    RedisAdvancedClusterCommands<String, String> commands         = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster     = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts         = mock(Accounts.class);
    DirectoryManager                             directoryManager = mock(DirectoryManager.class);
    DirectoryQueue                               directoryQueue   = mock(DirectoryQueue.class);
    Keys                                         keys             = mock(Keys.class);
    MessagesManager                              messagesManager  = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager  = mock(ProfilesManager.class);
    UUID                                         uuid             = UUID.randomUUID();
    Account                                      account          = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);
    when(accounts.get(eq(uuid))).thenReturn(Optional.of(account));

    AccountsManager   accountsManager = new AccountsManager(accounts, directoryManager, cacheCluster, directoryQueue, keys, messagesManager, usernamesManager, profilesManager);
    Optional<Account> retrieved       = accountsManager.get(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("Account3::" + uuid));
    verify(commands, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(commands, times(1)).set(eq("Account3::" + uuid.toString()), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).get(eq(uuid));
    verifyNoMoreInteractions(accounts);
  }

  @Test
  public void testGetAccountByNumberBrokenCache() {
    RedisAdvancedClusterCommands<String, String> commands         = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster     = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts         = mock(Accounts.class);
    DirectoryManager                             directoryManager = mock(DirectoryManager.class);
    DirectoryQueue                               directoryQueue   = mock(DirectoryQueue.class);
    Keys                                         keys             = mock(Keys.class);
    MessagesManager                              messagesManager  = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager  = mock(ProfilesManager.class);
    UUID                                         uuid             = UUID.randomUUID();
    Account                                      account          = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    when(commands.get(eq("AccountMap::+14152222222"))).thenThrow(new RedisException("Connection lost!"));
    when(accounts.get(eq("+14152222222"))).thenReturn(Optional.of(account));

    AccountsManager   accountsManager = new AccountsManager(accounts, directoryManager, cacheCluster, directoryQueue, keys, messagesManager, usernamesManager, profilesManager);
    Optional<Account> retrieved       = accountsManager.get("+14152222222");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("AccountMap::+14152222222"));
    verify(commands, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(commands, times(1)).set(eq("Account3::" + uuid.toString()), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).get(eq("+14152222222"));
    verifyNoMoreInteractions(accounts);
  }

  @Test
  public void testGetAccountByUuidBrokenCache() {
    RedisAdvancedClusterCommands<String, String> commands         = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster     = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts         = mock(Accounts.class);
    DirectoryManager                             directoryManager = mock(DirectoryManager.class);
    DirectoryQueue                               directoryQueue   = mock(DirectoryQueue.class);
    Keys                                         keys             = mock(Keys.class);
    MessagesManager                              messagesManager  = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager  = mock(ProfilesManager.class);
    UUID                                         uuid             = UUID.randomUUID();
    Account                                      account          = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    when(commands.get(eq("Account3::" + uuid))).thenThrow(new RedisException("Connection lost!"));
    when(accounts.get(eq(uuid))).thenReturn(Optional.of(account));

    AccountsManager   accountsManager = new AccountsManager(accounts, directoryManager, cacheCluster, directoryQueue, keys, messagesManager, usernamesManager, profilesManager);
    Optional<Account> retrieved       = accountsManager.get(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("Account3::" + uuid));
    verify(commands, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(commands, times(1)).set(eq("Account3::" + uuid.toString()), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).get(eq(uuid));
    verifyNoMoreInteractions(accounts);
  }


}
