/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotNull;

public class DynamicVerificationCodeStoreMigrationConfiguration {

  public enum WriteDestination {
    POSTGRES,
    DYNAMODB
  }

  @JsonProperty
  @NotNull
  private WriteDestination writeDestination = WriteDestination.POSTGRES;

  @JsonProperty
  private boolean readPostgres = true;

  @JsonProperty
  private boolean readDynamoDb = false;

  public WriteDestination getWriteDestination() {
    return writeDestination;
  }

  public void setWriteDestination(final WriteDestination writeDestination) {
    this.writeDestination = writeDestination;
  }

  public boolean isReadPostgres() {
    return readPostgres;
  }

  public void setReadPostgres(final boolean readPostgres) {
    this.readPostgres = readPostgres;
  }

  public boolean isReadDynamoDb() {
    return readDynamoDb;
  }

  public void setReadDynamoDb(final boolean readDynamoDb) {
    this.readDynamoDb = readDynamoDb;
  }
}
