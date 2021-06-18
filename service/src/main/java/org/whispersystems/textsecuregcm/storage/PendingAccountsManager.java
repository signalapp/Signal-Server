/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import java.util.Optional;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;

public class PendingAccountsManager {

  private final PendingAccounts pendingAccounts;
  private final VerificationCodeStoreDynamoDb pendingAccountsDynamoDb;
  private final DynamicConfigurationManager dynamicConfigurationManager;

  public PendingAccountsManager(
      final PendingAccounts pendingAccounts,
      final VerificationCodeStoreDynamoDb pendingAccountsDynamoDb,
      final DynamicConfigurationManager dynamicConfigurationManager) {

    this.pendingAccounts = pendingAccounts;
    this.pendingAccountsDynamoDb = pendingAccountsDynamoDb;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  public void store(String number, StoredVerificationCode code) {
    switch (dynamicConfigurationManager.getConfiguration().getPendingAccountsMigrationConfiguration().getWriteDestination()) {

      case POSTGRES:
        pendingAccounts.insert(number, code);
        break;

      case DYNAMODB:
        pendingAccountsDynamoDb.insert(number, code);
        break;
    }
  }

  public void remove(String number) {
    pendingAccounts.remove(number);
    pendingAccountsDynamoDb.remove(number);
  }

  public Optional<StoredVerificationCode> getCodeForNumber(String number) {
    final Optional<StoredVerificationCode> maybeCodeFromPostgres =
        dynamicConfigurationManager.getConfiguration().getPendingAccountsMigrationConfiguration().isReadPostgres()
            ? pendingAccounts.findForNumber(number)
            : Optional.empty();

    return maybeCodeFromPostgres.or(
        () -> dynamicConfigurationManager.getConfiguration().getPendingAccountsMigrationConfiguration().isReadDynamoDb()
            ? pendingAccountsDynamoDb.findForNumber(number)
            : Optional.empty());
  }
}
