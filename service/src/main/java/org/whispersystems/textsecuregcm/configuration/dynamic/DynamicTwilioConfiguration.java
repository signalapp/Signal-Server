package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;

public class DynamicTwilioConfiguration {

  @JsonProperty
  @NotNull
  private List<String> numbers = Collections.emptyList();

  public List<String> getNumbers() {
    return numbers;
  }

  @VisibleForTesting
  public void setNumbers(List<String> numbers) {
    this.numbers = numbers;
  }
}
