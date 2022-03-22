package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TurnUriConfiguration {
  @JsonProperty
  @NotNull
  private List<String> uris;

  /**
   * The weight of this entry for weighted random selection
   */
  @JsonProperty
  @Min(0)
  private long weight = 1;

  /**
   * Enrolled numbers will always get this uri list
   */
  private Set<String> enrolledNumbers = Collections.emptySet();

  public List<String> getUris() {
    return uris;
  }

  public long getWeight() {
    return weight;
  }

  public Set<String> getEnrolledNumbers() {
    return Collections.unmodifiableSet(enrolledNumbers);
  }
}
