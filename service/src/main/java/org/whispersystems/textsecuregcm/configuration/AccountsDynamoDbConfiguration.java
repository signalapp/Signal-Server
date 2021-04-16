package org.whispersystems.textsecuregcm.configuration;

import javax.validation.constraints.NotNull;

public class AccountsDynamoDbConfiguration extends DynamoDbConfiguration {

  @NotNull
  private String phoneNumberTableName;

  public String getPhoneNumberTableName() {
    return phoneNumberTableName;
  }
}
