/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class UpdateGambiaPniMappingsCommand extends AbstractSinglePassCrawlAccountsCommand {

  public UpdateGambiaPniMappingsCommand() {
    super("update-gambia-pni-mappings", "Maps legacy Gambian phone numbers and their new forms to the same PNI");
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final PhoneNumberIdentifiers phoneNumberIdentifiers = getCommandDependencies().phoneNumberIdentifiers();

    final Counter updatedPniMappingCounter = Metrics.counter(MetricsUtil.name(getClass(), "updatedPniMapping"));

    accounts
        .filter(account -> "GM".equalsIgnoreCase(Util.getRegion(account)))
        .flatMap(accountWithGambianNumber -> {
          final String e164 = accountWithGambianNumber.getNumber().orElseThrow();

          return Mono.fromFuture(() -> phoneNumberIdentifiers.setPni(e164,
              Util.getAlternateForms(e164),
              accountWithGambianNumber.getPhoneNumberIdentifier().orElseThrow()));
        })
        .doOnNext(_ -> updatedPniMappingCounter.increment())
        .then()
        .block();
  }
}
