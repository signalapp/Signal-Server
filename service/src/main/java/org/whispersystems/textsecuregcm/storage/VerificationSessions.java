/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.time.Clock;
import org.whispersystems.textsecuregcm.registration.VerificationSession;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class VerificationSessions extends SerializedExpireableJsonDynamoStore<VerificationSession> {

  public VerificationSessions(final DynamoDbAsyncClient dynamoDbClient, final String tableName, final Clock clock) {
    super(dynamoDbClient, tableName, clock);
  }
}
