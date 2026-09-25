/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.purchases;

import java.time.Instant;

/**
 * Payment details for a successful one-time payment specified by id
 *
 * @param id             The id of the payment in the payment processor
 * @param level          The level identifier of purchase
 * @param created        When the payment was created
 */
public record PaymentDetails(String id, ReceiptLevel level, Instant created) {}
