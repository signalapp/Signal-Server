package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotNull;

public class AccountsDynamoDbConfiguration extends DynamoDbConfiguration {

  @NotNull
  private String phoneNumberTableName;

  private int scanPageSize = 100;

  @JsonProperty
  public String getPhoneNumberTableName() {
    return phoneNumberTableName;
  }

  @JsonProperty
  public int getScanPageSize() {
    return scanPageSize;
  }

}
