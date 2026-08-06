/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import java.util.EnumMap;
import java.util.Map;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;
import org.whispersystems.textsecuregcm.util.EnumMapUtil;

public class IssuedReceiptsTableConfiguration extends DynamoDbTables.Table {

  private final byte[] generator;

  /// The maximum number of receipts that may be issued for a single subscription payment.
  private final EnumMap<PaymentProvider, Integer> maxReceiptsPerSubscriptionPayment;

  public IssuedReceiptsTableConfiguration(
      @JsonProperty("tableName") final String tableName,
      @JsonProperty("generator") final byte[] generator,
      @JsonProperty("maxReceiptsPerSubscriptionPayment") final Map<PaymentProvider, Integer> maxReceiptsPerSubscriptionPayment) {
    super(tableName);
    this.generator = generator;
    this.maxReceiptsPerSubscriptionPayment = EnumMapUtil.toCompleteEnumMap(PaymentProvider.class, maxReceiptsPerSubscriptionPayment);
  }

  @NotEmpty
  public byte[] getGenerator() {
    return generator;
  }

  public EnumMap<PaymentProvider, Integer> getMaxReceiptsPerSubscriptionPayment() {
    return maxReceiptsPerSubscriptionPayment;
  }
}
