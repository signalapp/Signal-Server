/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicVerificationCodeStoreMigrationConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicVerificationCodeStoreMigrationConfiguration.WriteDestination;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PendingAccountsManagerTest {

  private PendingAccounts postgresPendingAccounts;
  private VerificationCodeStoreDynamoDb dynamoDbPendingAccounts;
  private DynamicVerificationCodeStoreMigrationConfiguration migrationConfiguration;

  private PendingAccountsManager pendingAccountsManager;

  @BeforeEach
  void setUp() {
    final DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    migrationConfiguration = mock(DynamicVerificationCodeStoreMigrationConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getPendingAccountsMigrationConfiguration()).thenReturn(migrationConfiguration);

    postgresPendingAccounts = mock(PendingAccounts.class);
    dynamoDbPendingAccounts = mock(VerificationCodeStoreDynamoDb.class);

    pendingAccountsManager = new PendingAccountsManager(postgresPendingAccounts, dynamoDbPendingAccounts, dynamicConfigurationManager);
  }

  @Test
  void storePostgres() {
    final String number = "+18005551234";
    final StoredVerificationCode code = mock(StoredVerificationCode.class);

    when(migrationConfiguration.getWriteDestination()).thenReturn(WriteDestination.POSTGRES);

    pendingAccountsManager.store(number, code);

    verify(postgresPendingAccounts).insert(number, code);
    verify(dynamoDbPendingAccounts, never()).insert(any(), any());
  }

  @Test
  void storeDynamoDb() {
    final String number = "+18005551234";
    final StoredVerificationCode code = mock(StoredVerificationCode.class);

    when(migrationConfiguration.getWriteDestination()).thenReturn(WriteDestination.DYNAMODB);

    pendingAccountsManager.store(number, code);

    verify(dynamoDbPendingAccounts).insert(number, code);
    verify(postgresPendingAccounts, never()).insert(any(), any());
  }

  @Test
  void remove() {
    final String number = "+18005551234";

    pendingAccountsManager.remove(number);

    verify(postgresPendingAccounts).remove(number);
    verify(dynamoDbPendingAccounts).remove(number);
  }

  @Test
  void getCodeForNumber() {
    final String number = "+18005551234";

    final StoredVerificationCode postgresCode = mock(StoredVerificationCode.class);
    final StoredVerificationCode dynamoDbCode = mock(StoredVerificationCode.class);

    {
      when(migrationConfiguration.isReadPostgres()).thenReturn(false);
      when(migrationConfiguration.isReadDynamoDb()).thenReturn(false);
      when(postgresPendingAccounts.findForNumber(number)).thenReturn(Optional.empty());
      when(dynamoDbPendingAccounts.findForNumber(number)).thenReturn(Optional.empty());

      assertEquals(Optional.empty(), pendingAccountsManager.getCodeForNumber(number));

      when(migrationConfiguration.isReadPostgres()).thenReturn(true);

      assertEquals(Optional.empty(), pendingAccountsManager.getCodeForNumber(number));

      when(dynamoDbPendingAccounts.findForNumber(number)).thenReturn(Optional.of(dynamoDbCode));

      assertEquals(Optional.empty(), pendingAccountsManager.getCodeForNumber(number));

      when(postgresPendingAccounts.findForNumber(number)).thenReturn(Optional.of(postgresCode));

      assertEquals(Optional.of(postgresCode), pendingAccountsManager.getCodeForNumber(number));
    }

    {
      when(migrationConfiguration.isReadPostgres()).thenReturn(false);
      when(migrationConfiguration.isReadDynamoDb()).thenReturn(false);
      when(postgresPendingAccounts.findForNumber(number)).thenReturn(Optional.empty());
      when(dynamoDbPendingAccounts.findForNumber(number)).thenReturn(Optional.empty());

      assertEquals(Optional.empty(), pendingAccountsManager.getCodeForNumber(number));

      when(migrationConfiguration.isReadDynamoDb()).thenReturn(true);

      assertEquals(Optional.empty(), pendingAccountsManager.getCodeForNumber(number));

      when(postgresPendingAccounts.findForNumber(number)).thenReturn(Optional.of(postgresCode));

      assertEquals(Optional.empty(), pendingAccountsManager.getCodeForNumber(number));

      when(dynamoDbPendingAccounts.findForNumber(number)).thenReturn(Optional.of(dynamoDbCode));

      assertEquals(Optional.of(dynamoDbCode), pendingAccountsManager.getCodeForNumber(number));
    }

    {
      when(migrationConfiguration.isReadPostgres()).thenReturn(false);
      when(migrationConfiguration.isReadDynamoDb()).thenReturn(false);
      when(postgresPendingAccounts.findForNumber(number)).thenReturn(Optional.of(postgresCode));
      when(dynamoDbPendingAccounts.findForNumber(number)).thenReturn(Optional.of(dynamoDbCode));

      assertEquals(Optional.empty(), pendingAccountsManager.getCodeForNumber(number));

      when(migrationConfiguration.isReadDynamoDb()).thenReturn(true);

      assertEquals(Optional.of(dynamoDbCode), pendingAccountsManager.getCodeForNumber(number));

      when(migrationConfiguration.isReadDynamoDb()).thenReturn(false);
      when(migrationConfiguration.isReadPostgres()).thenReturn(true);

      assertEquals(Optional.of(postgresCode), pendingAccountsManager.getCodeForNumber(number));

      when(migrationConfiguration.isReadDynamoDb()).thenReturn(true);

      assertEquals(Optional.of(postgresCode), pendingAccountsManager.getCodeForNumber(number));
    }
  }
}
