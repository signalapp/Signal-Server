/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

public class TwilioConfiguration {

  @NotEmpty
  @JsonProperty
  private String accountId;

  @NotEmpty
  @JsonProperty
  private String accountToken;

  @NotNull
  @JsonProperty
  private List<String> numbers;

  @NotEmpty
  @JsonProperty
  private String localDomain;

  @JsonProperty
  private String messagingServicesId;

  @NotNull
  @Valid
  private CircuitBreakerConfiguration circuitBreaker = new CircuitBreakerConfiguration();

  @NotNull
  @Valid
  private RetryConfiguration retry = new RetryConfiguration();

  public String getAccountId() {
    return accountId;
  }

  @VisibleForTesting
  public void setAccountId(String accountId) {
    this.accountId = accountId;
  }

  public String getAccountToken() {
    return accountToken;
  }

  @VisibleForTesting
  public void setAccountToken(String accountToken) {
    this.accountToken = accountToken;
  }

  public List<String> getNumbers() {
    return numbers;
  }

  @VisibleForTesting
  public void setNumbers(List<String> numbers) {
    this.numbers = numbers;
  }

  public String getLocalDomain() {
    return localDomain;
  }

  @VisibleForTesting
  public void setLocalDomain(String localDomain) {
    this.localDomain = localDomain;
  }

  public String getMessagingServicesId() {
    return messagingServicesId;
  }

  @VisibleForTesting
  public void setMessagingServicesId(String messagingServicesId) {
    this.messagingServicesId = messagingServicesId;
  }

  public CircuitBreakerConfiguration getCircuitBreaker() {
    return circuitBreaker;
  }

  @VisibleForTesting
  public void setCircuitBreaker(CircuitBreakerConfiguration circuitBreaker) {
    this.circuitBreaker = circuitBreaker;
  }

  public RetryConfiguration getRetry() {
    return retry;
  }

  @VisibleForTesting
  public void setRetry(RetryConfiguration retry) {
    this.retry = retry;
  }
}
