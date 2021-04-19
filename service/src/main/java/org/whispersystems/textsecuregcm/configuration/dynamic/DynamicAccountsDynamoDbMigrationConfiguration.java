package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

public class DynamicAccountsDynamoDbMigrationConfiguration {

  @JsonProperty
  boolean backgroundMigrationEnabled;

  @JsonProperty
  int backgroundMigrationExecutorThreads = 1;

  @JsonProperty
  boolean deleteEnabled;

  @JsonProperty
  boolean writeEnabled;

  @JsonProperty
  boolean readEnabled;

  public boolean isBackgroundMigrationEnabled() {
    return backgroundMigrationEnabled;
  }

  public int getBackgroundMigrationExecutorThreads() {
    return backgroundMigrationExecutorThreads;
  }

  public void setDeleteEnabled(boolean deleteEnabled) {
    this.deleteEnabled = deleteEnabled;
  }

  public boolean isDeleteEnabled() {
    return deleteEnabled;
  }

  public void setWriteEnabled(boolean writeEnabled) {
    this.writeEnabled = writeEnabled;
  }

  public boolean isWriteEnabled() {
    return writeEnabled;
  }

  @VisibleForTesting
  public void setReadEnabled(boolean readEnabled) {
    this.readEnabled = readEnabled;
  }

  public boolean isReadEnabled() {
    return readEnabled;
  }
}
