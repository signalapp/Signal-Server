/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class DynamoDbTables {

  public static class Table {
    private final String tableName;

    @JsonCreator
    public Table(
        @JsonProperty("tableName") final String tableName) {
      this.tableName = tableName;
    }

    @NotEmpty
    public String getTableName() {
      return tableName;
    }
  }

  public static class TableWithExpiration extends Table {
    private final Duration expiration;

    @JsonCreator
    public TableWithExpiration(
        @JsonProperty("tableName") final String tableName,
        @JsonProperty("expiration") final Duration expiration) {
      super(tableName);
      this.expiration = expiration;
    }

    @NotNull
    public Duration getExpiration() {
      return expiration;
    }
  }

  private final IssuedReceiptsTableConfiguration issuedReceipts;
  private final TableWithExpiration redeemedReceipts;
  private final Table subscriptions;
  private final Table profiles;

  @JsonCreator
  public DynamoDbTables(
      @JsonProperty("issuedReceipts") final IssuedReceiptsTableConfiguration issuedReceipts,
      @JsonProperty("redeemedReceipts") final TableWithExpiration redeemedReceipts,
      @JsonProperty("subscriptions") final Table subscriptions,
      @JsonProperty("profiles") final Table profiles) {
    this.issuedReceipts = issuedReceipts;
    this.redeemedReceipts = redeemedReceipts;
    this.subscriptions = subscriptions;
    this.profiles = profiles;
  }

  @Valid
  @NotNull
  public IssuedReceiptsTableConfiguration getIssuedReceipts() {
    return issuedReceipts;
  }

  @Valid
  @NotNull
  public TableWithExpiration getRedeemedReceipts() {
    return redeemedReceipts;
  }

  @Valid
  @NotNull
  public Table getSubscriptions() {
    return subscriptions;
  }

  @Valid
  @NotNull
  public Table getProfiles() {
    return profiles;
  }
}
