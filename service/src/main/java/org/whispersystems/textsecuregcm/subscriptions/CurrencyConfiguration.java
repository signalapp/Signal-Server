package org.whispersystems.textsecuregcm.subscriptions;

import io.swagger.v3.oas.annotations.media.Schema;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Schema(description = "Configuration for a currency - use to present appropriate client interfaces")
public record CurrencyConfiguration(
    @Schema(description = "The minimum amount that may be submitted for a one-time donation in the currency")
    BigDecimal minimum,
    @Schema(description = "A map of numeric one-time donation level IDs to the list of default amounts to be presented")
    Map<Long, List<BigDecimal>> oneTime,
    @Schema(description = "A map of numeric subscription level IDs to the amount charged for that level")
    Map<Long, BigDecimal> subscription,
    @Schema(description = "A map of numeric backup level IDs to the amount charged for that level")
    Map<Long, BigDecimal> backupSubscription,
    @Schema(description = "The payment methods that support the given currency")
    List<PaymentMethod> supportedPaymentMethods) {}
