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

  /// The maximum number of receipts that may be issued for a single subscription payment.
  private final EnumMap<PaymentProvider, Integer> maxReceiptsPerSubscriptionPayment;

  public IssuedReceiptsTableConfiguration(
      @JsonProperty("tableName") final String tableName,
      @JsonProperty("expiration") final Duration expiration,
      @JsonProperty("generator") final byte[] generator,
      @JsonProperty("maxReceiptsPerSubscriptionPayment") final Map<PaymentProvider, Integer> maxReceiptsPerSubscriptionPayment) {
    super(tableName, expiration);
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
