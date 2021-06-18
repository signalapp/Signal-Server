/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import java.util.Optional;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;

public class PendingDevicesManager {

  private final PendingDevices pendingDevices;
  private final VerificationCodeStoreDynamoDb pendingDevicesDynamoDb;
  private final DynamicConfigurationManager dynamicConfigurationManager;

  public PendingDevicesManager(
      final PendingDevices pendingDevices,
      final VerificationCodeStoreDynamoDb pendingDevicesDynamoDb,
      final DynamicConfigurationManager dynamicConfigurationManager) {

    this.pendingDevices = pendingDevices;
    this.pendingDevicesDynamoDb = pendingDevicesDynamoDb;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  public void store(String number, StoredVerificationCode code) {
    switch (dynamicConfigurationManager.getConfiguration().getPendingDevicesMigrationConfiguration().getWriteDestination()) {
      case POSTGRES:
        pendingDevices.insert(number, code);
        break;

      case DYNAMODB:
        pendingDevicesDynamoDb.insert(number, code);
        break;
    }
  }

  public void remove(String number) {
    pendingDevices.remove(number);
    pendingDevicesDynamoDb.remove(number);
  }

  public Optional<StoredVerificationCode> getCodeForNumber(String number) {
    final Optional<StoredVerificationCode> maybeCodeFromPostgres =
        dynamicConfigurationManager.getConfiguration().getPendingDevicesMigrationConfiguration().isReadPostgres()
            ? pendingDevices.findForNumber(number)
            : Optional.empty();

    return maybeCodeFromPostgres.or(
        () -> dynamicConfigurationManager.getConfiguration().getPendingDevicesMigrationConfiguration().isReadDynamoDb()
            ? pendingDevicesDynamoDb.findForNumber(number)
            : Optional.empty());
  }
}
