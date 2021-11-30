/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamicProfileMigrationConfiguration {

  @JsonProperty
  private boolean dynamoDbDeleteEnabled = false;

  @JsonProperty
  private boolean dynamoDbWriteEnabled = false;

  @JsonProperty
  private boolean dynamoDbReadForComparisonEnabled = false;

  @JsonProperty
  private boolean dynamoDbReadPrimary = false;

  @JsonProperty
  private boolean logMismatches = false;

  public boolean isDynamoDbDeleteEnabled() {
    return dynamoDbDeleteEnabled;
  }

  public boolean isDynamoDbWriteEnabled() {
    return dynamoDbWriteEnabled;
  }

  public boolean isDynamoDbReadForComparisonEnabled() {
    return dynamoDbReadForComparisonEnabled;
  }

  public boolean isDynamoDbReadPrimary() {
    return dynamoDbReadPrimary;
  }

  public boolean isLogMismatches() {
    return logMismatches;
  }
}
