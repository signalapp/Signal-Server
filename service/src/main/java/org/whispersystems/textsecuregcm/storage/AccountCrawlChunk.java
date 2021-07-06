/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class AccountCrawlChunk {

  private final List<Account> accounts;
  @Nullable
  private final UUID lastUuid;

  public AccountCrawlChunk(final List<Account> accounts, @Nullable final UUID lastUuid) {
    this.accounts = accounts;
    this.lastUuid = lastUuid;
  }

  public List<Account> getAccounts() {
    return accounts;
  }

  public Optional<UUID> getLastUuid() {
    return Optional.ofNullable(lastUuid);
  }
}
