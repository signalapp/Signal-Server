/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.Util;

public class ReportMessageHelper {

  private static final Logger logger = LoggerFactory.getLogger(ReportMessageHelper.class);

  public static void reportMessage(final ServiceIdentifier sourceServiceIdentifier,
      final AciServiceIdentifier reporterServiceIdentifier,
      final UUID messageGuid,
      @Nullable final byte[] reportSpamToken,
      @Nullable final String userAgent,
      final AccountsManager accountsManager,
      final PhoneNumberIdentifiers phoneNumberIdentifiers,
      final ReportMessageManager reportMessageManager) {

    final Optional<String> maybeSourceNumber;
    final Optional<UUID> maybeSourcePni;
    final boolean sourceAccountDeleted;

    final Optional<Account> sourceAccount = accountsManager.getByServiceIdentifier(sourceServiceIdentifier);

    if (sourceAccount.isEmpty()) {
      logger.warn("Could not find source: {}", sourceServiceIdentifier);
      maybeSourcePni = accountsManager.findRecentlyDeletedPhoneNumberIdentifier(sourceServiceIdentifier.uuid());
      maybeSourceNumber = maybeSourcePni.flatMap(pni ->
          Util.getCanonicalNumber(phoneNumberIdentifiers.getPhoneNumber(pni).join()));
      sourceAccountDeleted = true;
    } else {
      maybeSourceNumber = sourceAccount.flatMap(Account::getNumber);
      maybeSourcePni = sourceAccount.flatMap(Account::getPhoneNumberIdentifier);
      sourceAccountDeleted = false;
    }

    reportMessageManager.report(maybeSourceNumber,
        sourceServiceIdentifier.uuid(),
        maybeSourcePni,
        messageGuid,
        reporterServiceIdentifier.uuid(),
        Optional.ofNullable(reportSpamToken),
        userAgent,
        sourceAccountDeleted);
  }
}
