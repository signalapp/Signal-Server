/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import javax.validation.constraints.NotEmpty;

public class IssuedReceiptsTableConfiguration extends DynamoDbTables.TableWithExpiration {

  private final byte[] generator;

  public IssuedReceiptsTableConfiguration(
      @JsonProperty("tableName") final String tableName,
      @JsonProperty("expiration") final Duration expiration,
      @JsonProperty("generator") final byte[] generator) {
    super(tableName, expiration);
    this.generator = generator;
  }

  @NotEmpty
  public byte[] getGenerator() {
    return generator;
  }
}
