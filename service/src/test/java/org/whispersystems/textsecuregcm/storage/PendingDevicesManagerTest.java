/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicVerificationCodeStoreMigrationConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicVerificationCodeStoreMigrationConfiguration.WriteDestination;

class PendingDevicesManagerTest {

  private PendingDevices postgresPendingDevices;
  private VerificationCodeStoreDynamoDb dynamoDbPendingDevices;
  private DynamicVerificationCodeStoreMigrationConfiguration migrationConfiguration;

  private PendingDevicesManager pendingDevicesManager;

  @BeforeEach
  void setUp() {
    final DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    migrationConfiguration = mock(DynamicVerificationCodeStoreMigrationConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getPendingDevicesMigrationConfiguration()).thenReturn(migrationConfiguration);

    postgresPendingDevices = mock(PendingDevices.class);
    dynamoDbPendingDevices = mock(VerificationCodeStoreDynamoDb.class);

    pendingDevicesManager = new PendingDevicesManager(postgresPendingDevices, dynamoDbPendingDevices, dynamicConfigurationManager);
  }

    @Test
    void storePostgres() {
    final String number = "+18005551234";
    final StoredVerificationCode code = mock(StoredVerificationCode.class);

    when(migrationConfiguration.getWriteDestination()).thenReturn(WriteDestination.POSTGRES);

    pendingDevicesManager.store(number, code);

    verify(postgresPendingDevices).insert(number, code);
    verify(dynamoDbPendingDevices, never()).insert(any(), any());
  }

  @Test
  void storeDynamoDb() {
    final String number = "+18005551234";
    final StoredVerificationCode code = mock(StoredVerificationCode.class);

    when(migrationConfiguration.getWriteDestination()).thenReturn(WriteDestination.DYNAMODB);

    pendingDevicesManager.store(number, code);

    verify(dynamoDbPendingDevices).insert(number, code);
    verify(postgresPendingDevices, never()).insert(any(), any());
  }

  @Test
  void remove() {
    final String number = "+18005551234";

    pendingDevicesManager.remove(number);

    verify(postgresPendingDevices).remove(number);
    verify(dynamoDbPendingDevices).remove(number);
  }

    @Test
    void getCodeForNumber() {
    final String number = "+18005551234";

    final StoredVerificationCode postgresCode = mock(StoredVerificationCode.class);
    final StoredVerificationCode dynamoDbCode = mock(StoredVerificationCode.class);

    {
      when(migrationConfiguration.isReadPostgres()).thenReturn(false);
      when(migrationConfiguration.isReadDynamoDb()).thenReturn(false);
      when(postgresPendingDevices.findForNumber(number)).thenReturn(Optional.empty());
      when(dynamoDbPendingDevices.findForNumber(number)).thenReturn(Optional.empty());

      assertEquals(Optional.empty(), pendingDevicesManager.getCodeForNumber(number));

      when(migrationConfiguration.isReadPostgres()).thenReturn(true);

      assertEquals(Optional.empty(), pendingDevicesManager.getCodeForNumber(number));

      when(dynamoDbPendingDevices.findForNumber(number)).thenReturn(Optional.of(dynamoDbCode));

      assertEquals(Optional.empty(), pendingDevicesManager.getCodeForNumber(number));

      when(postgresPendingDevices.findForNumber(number)).thenReturn(Optional.of(postgresCode));

      assertEquals(Optional.of(postgresCode), pendingDevicesManager.getCodeForNumber(number));
    }

    {
      when(migrationConfiguration.isReadPostgres()).thenReturn(false);
      when(migrationConfiguration.isReadDynamoDb()).thenReturn(false);
      when(postgresPendingDevices.findForNumber(number)).thenReturn(Optional.empty());
      when(dynamoDbPendingDevices.findForNumber(number)).thenReturn(Optional.empty());

      assertEquals(Optional.empty(), pendingDevicesManager.getCodeForNumber(number));

      when(migrationConfiguration.isReadDynamoDb()).thenReturn(true);

      assertEquals(Optional.empty(), pendingDevicesManager.getCodeForNumber(number));

      when(postgresPendingDevices.findForNumber(number)).thenReturn(Optional.of(postgresCode));

      assertEquals(Optional.empty(), pendingDevicesManager.getCodeForNumber(number));

      when(dynamoDbPendingDevices.findForNumber(number)).thenReturn(Optional.of(dynamoDbCode));

      assertEquals(Optional.of(dynamoDbCode), pendingDevicesManager.getCodeForNumber(number));
    }

    {
      when(migrationConfiguration.isReadPostgres()).thenReturn(false);
      when(migrationConfiguration.isReadDynamoDb()).thenReturn(false);
      when(postgresPendingDevices.findForNumber(number)).thenReturn(Optional.of(postgresCode));
      when(dynamoDbPendingDevices.findForNumber(number)).thenReturn(Optional.of(dynamoDbCode));

      assertEquals(Optional.empty(), pendingDevicesManager.getCodeForNumber(number));

      when(migrationConfiguration.isReadDynamoDb()).thenReturn(true);

      assertEquals(Optional.of(dynamoDbCode), pendingDevicesManager.getCodeForNumber(number));

      when(migrationConfiguration.isReadDynamoDb()).thenReturn(false);
      when(migrationConfiguration.isReadPostgres()).thenReturn(true);

      assertEquals(Optional.of(postgresCode), pendingDevicesManager.getCodeForNumber(number));

      when(migrationConfiguration.isReadDynamoDb()).thenReturn(true);

      assertEquals(Optional.of(postgresCode), pendingDevicesManager.getCodeForNumber(number));
    }
  }
}
