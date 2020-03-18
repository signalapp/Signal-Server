package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.util.Strings;
import io.dropwizard.validation.ValidationMethod;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

public class GcpAttachmentsConfiguration {

  @NotEmpty
  @JsonProperty
  private String domain;

  @NotEmpty
  @JsonProperty
  private String email;

  @JsonProperty
  @Min(1)
  private int maxSizeInBytes;

  @JsonProperty
  private String pathPrefix;

  @NotEmpty
  @JsonProperty
  private String rsaSigningKey;

  public String getDomain() {
    return domain;
  }

  public String getEmail() {
    return email;
  }

  public int getMaxSizeInBytes() {
    return maxSizeInBytes;
  }

  public String getPathPrefix() {
    return pathPrefix;
  }

  public String getRsaSigningKey() {
    return rsaSigningKey;
  }

  @SuppressWarnings("unused")
  @ValidationMethod(message = "pathPrefix must be empty or start with /")
  public boolean isPathPrefixValid() {
    return Strings.isNullOrEmpty(pathPrefix) || pathPrefix.startsWith("/");
  }
}
