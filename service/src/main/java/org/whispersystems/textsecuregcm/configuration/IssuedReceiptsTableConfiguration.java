/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.util.EnumMapUtil;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

public class IssuedReceiptsTableConfiguration extends DynamoDbTables.TableWithExpiration {

  private final byte[] generator;

  /**
   * The maximum number of issued receipts the issued receipt manager should issue for a particular itemId
   */
  private final EnumMap<PaymentProvider, Integer> maxIssuedReceiptsPerPaymentId;

  public IssuedReceiptsTableConfiguration(
      @JsonProperty("tableName") final String tableName,
      @JsonProperty("expiration") final Duration expiration,
      @JsonProperty("generator") final byte[] generator,
      @JsonProperty("maxIssuedReceiptsPerPaymentId") final Map<PaymentProvider, Integer> maxIssuedReceiptsPerPaymentId) {
    super(tableName, expiration);
    this.generator = generator;
    this.maxIssuedReceiptsPerPaymentId = EnumMapUtil.toCompleteEnumMap(PaymentProvider.class, maxIssuedReceiptsPerPaymentId);
  }

  @NotEmpty
  public byte[] getGenerator() {
    return generator;
  }

  public EnumMap<PaymentProvider, Integer> getmaxIssuedReceiptsPerPaymentId() {
    return maxIssuedReceiptsPerPaymentId;
  }
}
