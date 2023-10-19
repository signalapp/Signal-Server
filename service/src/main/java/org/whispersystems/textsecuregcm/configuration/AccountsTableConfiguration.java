package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.whispersystems.textsecuregcm.configuration.DynamoDbTables.Table;
import javax.validation.constraints.NotBlank;

public class AccountsTableConfiguration extends Table {

  private final String phoneNumberTableName;
  private final String phoneNumberIdentifierTableName;
  private final String usernamesTableName;

  @JsonCreator
  public AccountsTableConfiguration(
      @JsonProperty("tableName") final String tableName,
      @JsonProperty("phoneNumberTableName") final String phoneNumberTableName,
      @JsonProperty("phoneNumberIdentifierTableName") final String phoneNumberIdentifierTableName,
      @JsonProperty("usernamesTableName") final String usernamesTableName) {

    super(tableName);

    this.phoneNumberTableName = phoneNumberTableName;
    this.phoneNumberIdentifierTableName = phoneNumberIdentifierTableName;
    this.usernamesTableName = usernamesTableName;
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
}
