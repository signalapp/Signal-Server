/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

public class AccountDatabaseCrawlerRestartException extends Exception {
  public AccountDatabaseCrawlerRestartException(String s) {
    super(s);
  }

  public AccountDatabaseCrawlerRestartException(Exception e) {
    super(e);
  }
}
