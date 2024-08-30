/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.annotation.Nullable;

/**
 * Information about a charge failure.
 * <p>
 * This is returned directly from {@link org.whispersystems.textsecuregcm.controllers.SubscriptionController}, so modify
 * with care.
 */
@Schema(description = """
      Meaningfully interpreting chargeFailure response fields requires inspecting the processor field first.

      For Stripe, code will be one of the [codes defined here](https://stripe.com/docs/api/charges/object#charge_object-failure_code),
      while message [may contain a further textual description](https://stripe.com/docs/api/charges/object#charge_object-failure_message).
      The outcome fields are nullable, but present values will directly map to Stripe [response properties](https://stripe.com/docs/api/charges/object#charge_object-outcome-network_status)

      For Braintree, the outcome fields will be null. The code and message will contain one of
        - a processor decline code (as a string) in code, and associated text in message, as defined this [table](https://developer.paypal.com/braintree/docs/reference/general/processor-responses/authorization-responses)
        - `gateway` in code, with a [reason](https://developer.paypal.com/braintree/articles/control-panel/transactions/gateway-rejections) in message
        - `code` = "unknown", message = "unknown"

      IAP payment processors will never include charge failure information, and detailed order information should be
      retrieved from the payment processor directly
    """)
public record ChargeFailure(
    @Schema(description = """
        See [Stripe failure codes](https://stripe.com/docs/api/charges/object#charge_object-failure_code) or
        [Braintree decline codes](https://developer.paypal.com/braintree/docs/reference/general/processor-responses/authorization-responses#decline-codes)
        depending on which processor was used
        """)
    String code,

    @Schema(description = """
        See [Stripe failure codes](https://stripe.com/docs/api/charges/object#charge_object-failure_code) or
        [Braintree decline codes](https://developer.paypal.com/braintree/docs/reference/general/processor-responses/authorization-responses#decline-codes)
        depending on which processor was used
        """)
    String message,

    @Schema(externalDocs = @ExternalDocumentation(description = "Outcome Network Status", url = "https://stripe.com/docs/api/charges/object#charge_object-outcome-network_status"))
    @Nullable String outcomeNetworkStatus,

    @Schema(externalDocs = @ExternalDocumentation(description = "Outcome Reason", url = "https://stripe.com/docs/api/charges/object#charge_object-outcome-reason"))
    @Nullable String outcomeReason,

    @Schema(externalDocs = @ExternalDocumentation(description = "Outcome Type", url = "https://stripe.com/docs/api/charges/object#charge_object-outcome-type"))
    @Nullable String outcomeType) {}
