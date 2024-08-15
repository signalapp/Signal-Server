/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.subscriptions;

import java.time.Instant;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Payment details for a one-time payment specified by id
 *
 * @param id             The id of the payment in the payment processor
 * @param customMetadata Any custom metadata attached to the payment
 * @param status         The status of the payment in the payment processor
 * @param created        When the payment was created
 * @param chargeFailure  If present, additional information about why the payment failed. Will not be set if the status
 *                       is not {@link PaymentStatus#SUCCEEDED}
 */
public record PaymentDetails(String id,
                             Map<String, String> customMetadata,
                             PaymentStatus status,
                             Instant created,
                             @Nullable ChargeFailure chargeFailure) {}
