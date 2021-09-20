package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamicAccountsDynamoDbMigrationConfiguration {

  @JsonProperty
  int dynamoCrawlerScanPageSize = 10;

  // TODO move out of "migration" configuration
  public int getDynamoCrawlerScanPageSize() {
    return dynamoCrawlerScanPageSize;
  }

}
