/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import io.lettuce.core.RedisException;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicAccountsDynamoDbMigrationConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.securebackup.SecureBackupClient;
import org.whispersystems.textsecuregcm.securestorage.SecureStorageClient;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.AccountsDynamoDb;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.KeysDynamoDb;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;

class AccountsManagerTest {

  private DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
  private ExperimentEnrollmentManager experimentEnrollmentManager = mock(ExperimentEnrollmentManager.class);

  @BeforeEach
  void setup() {

    DynamicConfiguration dynamicConfiguration = new DynamicConfiguration();

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByNumberInCache(final boolean dynamoEnabled) {
    RedisAdvancedClusterCommands<String, String> commands            = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster        = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts            = mock(Accounts.class);
    AccountsDynamoDb                             accountsDynamoDb    = mock(AccountsDynamoDb.class);
    DirectoryQueue                               directoryQueue      = mock(DirectoryQueue.class);
    KeysDynamoDb                                 keysDynamoDb        = mock(KeysDynamoDb.class);
    MessagesManager                              messagesManager     = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager    = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager     = mock(ProfilesManager.class);
    SecureBackupClient                           secureBackupClient  = mock(SecureBackupClient.class);
    SecureStorageClient                          secureStorageClient = mock(SecureStorageClient.class);

    UUID uuid = UUID.randomUUID();

    enableDynamo(dynamoEnabled);

    when(commands.get(eq("AccountMap::+14152222222"))).thenReturn(uuid.toString());
    when(commands.get(eq("Account3::" + uuid.toString()))).thenReturn("{\"number\": \"+14152222222\", \"name\": \"test\"}");

    AccountsManager   accountsManager = new AccountsManager(accounts, accountsDynamoDb, cacheCluster, directoryQueue, keysDynamoDb, messagesManager, usernamesManager, profilesManager, secureStorageClient, secureBackupClient, experimentEnrollmentManager, dynamicConfigurationManager);
    Optional<Account> account         = accountsManager.get("+14152222222");

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(account.get().getProfileName(), "test");

    verify(commands, times(1)).get(eq("AccountMap::+14152222222"));
    verify(commands, times(1)).get(eq("Account3::" + uuid.toString()));
    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(accounts);

    verifyZeroInteractions(accountsDynamoDb);
  }

  private void enableDynamo(boolean dynamoEnabled) {
    final DynamicAccountsDynamoDbMigrationConfiguration config = dynamicConfigurationManager.getConfiguration()
        .getAccountsDynamoDbMigrationConfiguration();

    config.setDeleteEnabled(dynamoEnabled);
    config.setReadEnabled(dynamoEnabled);
    config.setWriteEnabled(dynamoEnabled);

    when(experimentEnrollmentManager.isEnrolled(any(UUID.class), anyString()))
        .thenReturn(dynamoEnabled);

  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByUuidInCache(boolean dynamoEnabled) {
    RedisAdvancedClusterCommands<String, String> commands            = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster        = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts            = mock(Accounts.class);
    AccountsDynamoDb                             accountsDynamoDb    = mock(AccountsDynamoDb.class);
    DirectoryQueue                               directoryQueue      = mock(DirectoryQueue.class);
    KeysDynamoDb                                 keysDynamoDb        = mock(KeysDynamoDb.class);
    MessagesManager                              messagesManager     = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager    = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager     = mock(ProfilesManager.class);
    SecureBackupClient                           secureBackupClient  = mock(SecureBackupClient.class);
    SecureStorageClient                          secureStorageClient = mock(SecureStorageClient.class);

    UUID uuid = UUID.randomUUID();

    enableDynamo(dynamoEnabled);

    when(commands.get(eq("Account3::" + uuid.toString()))).thenReturn("{\"number\": \"+14152222222\", \"name\": \"test\"}");

    AccountsManager   accountsManager = new AccountsManager(accounts, accountsDynamoDb, cacheCluster, directoryQueue, keysDynamoDb, messagesManager, usernamesManager, profilesManager, secureStorageClient, secureBackupClient, experimentEnrollmentManager, dynamicConfigurationManager);
    Optional<Account> account         = accountsManager.get(uuid);

    assertTrue(account.isPresent());
    assertEquals(account.get().getNumber(), "+14152222222");
    assertEquals(account.get().getUuid(), uuid);
    assertEquals(account.get().getProfileName(), "test");

    verify(commands, times(1)).get(eq("Account3::" + uuid.toString()));
    verifyNoMoreInteractions(commands);
    verifyNoMoreInteractions(accounts);

    verifyZeroInteractions(accountsDynamoDb);
  }


  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByNumberNotInCache(boolean dynamoEnabled) {
    RedisAdvancedClusterCommands<String, String> commands            = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster        = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts            = mock(Accounts.class);
    AccountsDynamoDb                             accountsDynamoDb    = mock(AccountsDynamoDb.class);
    DirectoryQueue                               directoryQueue      = mock(DirectoryQueue.class);
    KeysDynamoDb                                 keysDynamoDb        = mock(KeysDynamoDb.class);
    MessagesManager                              messagesManager     = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager    = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager     = mock(ProfilesManager.class);
    SecureBackupClient                           secureBackupClient  = mock(SecureBackupClient.class);
    SecureStorageClient                          secureStorageClient = mock(SecureStorageClient.class);
    UUID                                         uuid                = UUID.randomUUID();
    Account                                      account             = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    enableDynamo(dynamoEnabled);

    when(commands.get(eq("AccountMap::+14152222222"))).thenReturn(null);
    when(accounts.get(eq("+14152222222"))).thenReturn(Optional.of(account));

    AccountsManager   accountsManager = new AccountsManager(accounts, accountsDynamoDb, cacheCluster, directoryQueue, keysDynamoDb, messagesManager, usernamesManager, profilesManager, secureStorageClient, secureBackupClient, experimentEnrollmentManager, dynamicConfigurationManager);
    Optional<Account> retrieved       = accountsManager.get("+14152222222");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("AccountMap::+14152222222"));
    verify(commands, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(commands, times(1)).set(eq("Account3::" + uuid.toString()), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).get(eq("+14152222222"));
    verifyNoMoreInteractions(accounts);

    verify(accountsDynamoDb, dynamoEnabled ? times(1) : never())
        .get(eq("+14152222222"));
    verifyNoMoreInteractions(accountsDynamoDb);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByUuidNotInCache(boolean dynamoEnabled) {
    RedisAdvancedClusterCommands<String, String> commands            = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster        = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts            = mock(Accounts.class);
    AccountsDynamoDb                             accountsDynamoDb    = mock(AccountsDynamoDb.class);
    DirectoryQueue                               directoryQueue      = mock(DirectoryQueue.class);
    KeysDynamoDb                                 keysDynamoDb        = mock(KeysDynamoDb.class);
    MessagesManager                              messagesManager     = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager    = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager     = mock(ProfilesManager.class);
    SecureBackupClient                           secureBackupClient  = mock(SecureBackupClient.class);
    SecureStorageClient                          secureStorageClient = mock(SecureStorageClient.class);
    UUID                                         uuid                = UUID.randomUUID();
    Account                                      account             = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    enableDynamo(dynamoEnabled);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);
    when(accounts.get(eq(uuid))).thenReturn(Optional.of(account));

    AccountsManager   accountsManager = new AccountsManager(accounts, accountsDynamoDb, cacheCluster, directoryQueue, keysDynamoDb, messagesManager, usernamesManager, profilesManager, secureStorageClient, secureBackupClient, experimentEnrollmentManager, dynamicConfigurationManager);
    Optional<Account> retrieved       = accountsManager.get(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("Account3::" + uuid));
    verify(commands, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(commands, times(1)).set(eq("Account3::" + uuid.toString()), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).get(eq(uuid));
    verifyNoMoreInteractions(accounts);

    verify(accountsDynamoDb, dynamoEnabled ? times(1) : never()).get(eq(uuid));
    verifyNoMoreInteractions(accountsDynamoDb);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByNumberBrokenCache(boolean dynamoEnabled) {
    RedisAdvancedClusterCommands<String, String> commands            = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster        = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts            = mock(Accounts.class);
    AccountsDynamoDb                             accountsDynamoDb    = mock(AccountsDynamoDb.class);
    DirectoryQueue                               directoryQueue      = mock(DirectoryQueue.class);
    KeysDynamoDb                                 keysDynamoDb        = mock(KeysDynamoDb.class);
    MessagesManager                              messagesManager     = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager    = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager     = mock(ProfilesManager.class);
    SecureBackupClient                           secureBackupClient  = mock(SecureBackupClient.class);
    SecureStorageClient                          secureStorageClient = mock(SecureStorageClient.class);
    UUID                                         uuid                = UUID.randomUUID();
    Account                                      account             = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    enableDynamo(dynamoEnabled);

    when(commands.get(eq("AccountMap::+14152222222"))).thenThrow(new RedisException("Connection lost!"));
    when(accounts.get(eq("+14152222222"))).thenReturn(Optional.of(account));

    AccountsManager   accountsManager = new AccountsManager(accounts, accountsDynamoDb, cacheCluster, directoryQueue, keysDynamoDb, messagesManager, usernamesManager, profilesManager, secureStorageClient, secureBackupClient, experimentEnrollmentManager, dynamicConfigurationManager);
    Optional<Account> retrieved       = accountsManager.get("+14152222222");

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("AccountMap::+14152222222"));
    verify(commands, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(commands, times(1)).set(eq("Account3::" + uuid.toString()), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).get(eq("+14152222222"));
    verifyNoMoreInteractions(accounts);

    verify(accountsDynamoDb, dynamoEnabled ? times(1) : never()).get(eq("+14152222222"));
    verifyNoMoreInteractions(accountsDynamoDb);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetAccountByUuidBrokenCache(boolean dynamoEnabled) {
    RedisAdvancedClusterCommands<String, String> commands            = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster        = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts            = mock(Accounts.class);
    AccountsDynamoDb                             accountsDynamoDb    = mock(AccountsDynamoDb.class);
    DirectoryQueue                               directoryQueue      = mock(DirectoryQueue.class);
    KeysDynamoDb                                 keysDynamoDb        = mock(KeysDynamoDb.class);
    MessagesManager                              messagesManager     = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager    = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager     = mock(ProfilesManager.class);
    SecureBackupClient                           secureBackupClient  = mock(SecureBackupClient.class);
    SecureStorageClient                          secureStorageClient = mock(SecureStorageClient.class);
    UUID                                         uuid                = UUID.randomUUID();
    Account                                      account             = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    enableDynamo(dynamoEnabled);

    when(commands.get(eq("Account3::" + uuid))).thenThrow(new RedisException("Connection lost!"));
    when(accounts.get(eq(uuid))).thenReturn(Optional.of(account));

    AccountsManager   accountsManager = new AccountsManager(accounts, accountsDynamoDb, cacheCluster, directoryQueue, keysDynamoDb, messagesManager, usernamesManager, profilesManager, secureStorageClient, secureBackupClient, experimentEnrollmentManager, dynamicConfigurationManager);
    Optional<Account> retrieved       = accountsManager.get(uuid);

    assertTrue(retrieved.isPresent());
    assertSame(retrieved.get(), account);

    verify(commands, times(1)).get(eq("Account3::" + uuid));
    verify(commands, times(1)).set(eq("AccountMap::+14152222222"), eq(uuid.toString()));
    verify(commands, times(1)).set(eq("Account3::" + uuid.toString()), anyString());
    verifyNoMoreInteractions(commands);

    verify(accounts, times(1)).get(eq(uuid));
    verifyNoMoreInteractions(accounts);

    verify(accountsDynamoDb, dynamoEnabled ? times(1) : never()).get(eq(uuid));
    verifyNoMoreInteractions(accountsDynamoDb);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testUpdate_dynamoDbMigration(boolean dynamoEnabled) {
    RedisAdvancedClusterCommands<String, String> commands            = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster        = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts            = mock(Accounts.class);
    AccountsDynamoDb                             accountsDynamoDb    = mock(AccountsDynamoDb.class);
    DirectoryQueue                               directoryQueue      = mock(DirectoryQueue.class);
    KeysDynamoDb                                 keysDynamoDb        = mock(KeysDynamoDb.class);
    MessagesManager                              messagesManager     = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager    = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager     = mock(ProfilesManager.class);
    SecureBackupClient                           secureBackupClient  = mock(SecureBackupClient.class);
    SecureStorageClient                          secureStorageClient = mock(SecureStorageClient.class);
    UUID                                         uuid                = UUID.randomUUID();
    Account                                      account             = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    enableDynamo(dynamoEnabled);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);

    AccountsManager   accountsManager = new AccountsManager(accounts, accountsDynamoDb, cacheCluster, directoryQueue, keysDynamoDb, messagesManager, usernamesManager, profilesManager, secureStorageClient, secureBackupClient, experimentEnrollmentManager, dynamicConfigurationManager);

    assertEquals(0, account.getDynamoDbMigrationVersion());

    accountsManager.update(account);

    assertEquals(1, account.getDynamoDbMigrationVersion());

    verify(accounts, times(1)).update(account);
    verifyNoMoreInteractions(accounts);

    verify(accountsDynamoDb, dynamoEnabled ? times(1) : never()).update(account);
    verifyNoMoreInteractions(accountsDynamoDb);
  }

  @Test
  void testUpdate_dynamoConditionFailed() {
    RedisAdvancedClusterCommands<String, String> commands            = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster        = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts            = mock(Accounts.class);
    AccountsDynamoDb                             accountsDynamoDb    = mock(AccountsDynamoDb.class);
    DirectoryQueue                               directoryQueue      = mock(DirectoryQueue.class);
    KeysDynamoDb                                 keysDynamoDb        = mock(KeysDynamoDb.class);
    MessagesManager                              messagesManager     = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager    = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager     = mock(ProfilesManager.class);
    SecureBackupClient                           secureBackupClient  = mock(SecureBackupClient.class);
    SecureStorageClient                          secureStorageClient = mock(SecureStorageClient.class);
    UUID                                         uuid                = UUID.randomUUID();
    Account                                      account             = new Account("+14152222222", uuid, new HashSet<>(), new byte[16]);

    enableDynamo(true);

    when(commands.get(eq("Account3::" + uuid))).thenReturn(null);
    doThrow(ConditionalCheckFailedException.class).when(accountsDynamoDb).update(any(Account.class));

    AccountsManager   accountsManager = new AccountsManager(accounts, accountsDynamoDb, cacheCluster, directoryQueue, keysDynamoDb, messagesManager, usernamesManager, profilesManager, secureStorageClient, secureBackupClient, experimentEnrollmentManager, dynamicConfigurationManager);

    assertEquals(0, account.getDynamoDbMigrationVersion());

    accountsManager.update(account);

    assertEquals(1, account.getDynamoDbMigrationVersion());

    verify(accounts, times(1)).update(account);
    verifyNoMoreInteractions(accounts);

    verify(accountsDynamoDb, times(1)).update(account);
    verify(accountsDynamoDb, times(1)).create(account);
    verifyNoMoreInteractions(accountsDynamoDb);
  }

  @Test
  void testCompareAccounts() {
    RedisAdvancedClusterCommands<String, String> commands            = mock(RedisAdvancedClusterCommands.class);
    FaultTolerantRedisCluster                    cacheCluster        = RedisClusterHelper.buildMockRedisCluster(commands);
    Accounts                                     accounts            = mock(Accounts.class);
    AccountsDynamoDb                             accountsDynamoDb    = mock(AccountsDynamoDb.class);
    DirectoryQueue                               directoryQueue      = mock(DirectoryQueue.class);
    KeysDynamoDb                                 keysDynamoDb        = mock(KeysDynamoDb.class);
    MessagesManager                              messagesManager     = mock(MessagesManager.class);
    UsernamesManager                             usernamesManager    = mock(UsernamesManager.class);
    ProfilesManager                              profilesManager     = mock(ProfilesManager.class);
    SecureBackupClient                           secureBackupClient  = mock(SecureBackupClient.class);
    SecureStorageClient                          secureStorageClient = mock(SecureStorageClient.class);

    AccountsManager   accountsManager = new AccountsManager(accounts, accountsDynamoDb, cacheCluster, directoryQueue, keysDynamoDb, messagesManager, usernamesManager, profilesManager, secureStorageClient, secureBackupClient, experimentEnrollmentManager, dynamicConfigurationManager);

    assertEquals(0, accountsManager.compareAccounts(Optional.empty(), Optional.empty()));

    final UUID uuidA = UUID.randomUUID();
    final Account a1 = new Account("+14152222222", uuidA, new HashSet<>(), new byte[16]);

    assertEquals(1, accountsManager.compareAccounts(Optional.empty(), Optional.of(a1)));

    final Account a2 = new Account("+14152222222", uuidA, new HashSet<>(), new byte[16]);

    assertEquals(0, accountsManager.compareAccounts(Optional.of(a1), Optional.of(a2)));

    a2.setProfileName("name");

    assertTrue(0 < accountsManager.compareAccounts(Optional.of(a1), Optional.of(a2)));
  }
}
