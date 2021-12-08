package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotBlank;
import org.whispersystems.textsecuregcm.configuration.DynamoDbTables.Table;

public class DeletedAccountsTableConfiguration extends Table {

  private final String needsReconciliationIndexName;

  @JsonCreator
  public DeletedAccountsTableConfiguration(
      @JsonProperty("tableName") final String tableName,
      @JsonProperty("needsReconciliationIndexName") final String needsReconciliationIndexName) {

    super(tableName);
    this.needsReconciliationIndexName = needsReconciliationIndexName;
  }

  @NotBlank
  public String getNeedsReconciliationIndexName() {
    return needsReconciliationIndexName;
  }
}
