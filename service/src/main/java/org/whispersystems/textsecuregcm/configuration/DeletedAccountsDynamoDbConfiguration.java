package org.whispersystems.textsecuregcm.configuration;

import javax.validation.constraints.NotNull;

public class DeletedAccountsDynamoDbConfiguration extends DynamoDbConfiguration {

  @NotNull
  private String needsReconciliationIndexName;

  public String getNeedsReconciliationIndexName() {
    return needsReconciliationIndexName;
  }
}
