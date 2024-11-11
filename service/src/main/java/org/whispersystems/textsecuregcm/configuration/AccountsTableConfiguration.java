package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import org.whispersystems.textsecuregcm.configuration.DynamoDbTables.Table;

public class AccountsTableConfiguration extends Table {

  private final String phoneNumberTableName;
  private final String phoneNumberIdentifierTableName;
  private final String usernamesTableName;
  private final String usedLinkDeviceTokensTableName;

  @JsonCreator
  public AccountsTableConfiguration(
      @JsonProperty("tableName") final String tableName,
      @JsonProperty("phoneNumberTableName") final String phoneNumberTableName,
      @JsonProperty("phoneNumberIdentifierTableName") final String phoneNumberIdentifierTableName,
      @JsonProperty("usernamesTableName") final String usernamesTableName,
      @JsonProperty("usedLinkDeviceTokensTableName") final String usedLinkDeviceTokensTableName) {

    super(tableName);

    this.phoneNumberTableName = phoneNumberTableName;
    this.phoneNumberIdentifierTableName = phoneNumberIdentifierTableName;
    this.usernamesTableName = usernamesTableName;
    this.usedLinkDeviceTokensTableName = usedLinkDeviceTokensTableName;
  }

  @NotBlank
  public String getPhoneNumberTableName() {
    return phoneNumberTableName;
  }

  @NotBlank
  public String getPhoneNumberIdentifierTableName() {
    return phoneNumberIdentifierTableName;
  }

  @NotBlank
  public String getUsernamesTableName() {
    return usernamesTableName;
  }

  @NotBlank
  public String getUsedLinkDeviceTokensTableName() {
    return usedLinkDeviceTokensTableName;
  }
}
