package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Configuration used to interact with a cdn via HTTP
 */
public class ClientCdnConfiguration {

  /**
   * Map from cdn number to the base url for attachments.
   * <p>
   * For example, if an attachment with the id 'abc' can be retrieved from cdn 2 at https://example.org/attachments/abc,
   * the attachment url for 2 should https://example.org/attachments
   */
  @JsonProperty
  @NotNull
  Map<Integer, @NotBlank String> attachmentUrls;

  @JsonProperty
  @NotNull
  @NotEmpty List<@NotBlank String> caCertificates = new ArrayList<>();

  @JsonProperty
  @NotNull
  CircuitBreakerConfiguration circuitBreaker = new CircuitBreakerConfiguration();

  @JsonProperty
  @NotNull
  RetryConfiguration retry = new RetryConfiguration();

  public List<String> getCaCertificates() {
    return caCertificates;
  }

  public CircuitBreakerConfiguration getCircuitBreaker() {
    return circuitBreaker;
  }

  public RetryConfiguration getRetry() {
    return retry;
  }

  public Map<Integer, String> getAttachmentUrls() {
    return attachmentUrls;
  }
}
