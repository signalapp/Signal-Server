/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import java.util.List;

import javax.validation.constraints.NotNull;

public record DynamicMessagesConfiguration(@NotNull List<DynamoKeyScheme> dynamoKeySchemes) {
  public enum DynamoKeyScheme {
      TRADITIONAL,
      LAZY_DELETION;
  }

  public DynamicMessagesConfiguration() {
    this(List.of(DynamoKeyScheme.TRADITIONAL));
  }

  public DynamoKeyScheme writeKeyScheme() {
    return dynamoKeySchemes().getLast();
  }
}
