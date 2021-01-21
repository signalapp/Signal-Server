/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.time.Duration;

public class MessageDynamoDbConfiguration extends DynamoDbConfiguration {

  private Duration timeToLive = Duration.ofDays(7);

  @Valid
  public Duration getTimeToLive() {
    return timeToLive;
  }
}
