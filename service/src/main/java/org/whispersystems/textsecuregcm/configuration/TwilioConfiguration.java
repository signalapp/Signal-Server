/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class TwilioConfiguration {

  @NotEmpty
  private String accountId;

  @NotEmpty
  private String accountToken;

  @NotEmpty
  private String localDomain;

  @NotEmpty
  private String messagingServiceSid;

  @NotEmpty
  private String nanpaMessagingServiceSid;

  @NotEmpty
  private String verifyServiceSid;

  @NotNull
  @Valid
  private CircuitBreakerConfiguration circuitBreaker = new CircuitBreakerConfiguration();

  @NotNull
  @Valid
  private RetryConfiguration retry = new RetryConfiguration();

  @Valid
  private TwilioVerificationTextConfiguration defaultClientVerificationTexts;

  @Valid
  private Map<String,TwilioVerificationTextConfiguration> regionalClientVerificationTexts = Collections.emptyMap();

  @NotEmpty
  private String androidAppHash;

  @NotEmpty
  private String verifyServiceFriendlyName;

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
  public String getLocalDomain() {
    return localDomain;
  }

  @VisibleForTesting
  public void setLocalDomain(String localDomain) {
    this.localDomain = localDomain;
  }

  public String getMessagingServiceSid() {
    return messagingServiceSid;
  }

  @VisibleForTesting
  public void setMessagingServiceSid(String messagingServiceSid) {
    this.messagingServiceSid = messagingServiceSid;
  }

  public String getNanpaMessagingServiceSid() {
    return nanpaMessagingServiceSid;
  }

  @VisibleForTesting
  public void setNanpaMessagingServiceSid(String nanpaMessagingServiceSid) {
    this.nanpaMessagingServiceSid = nanpaMessagingServiceSid;
  }

  public String getVerifyServiceSid() {
    return verifyServiceSid;
  }

  @VisibleForTesting
  public void setVerifyServiceSid(String verifyServiceSid) {
    this.verifyServiceSid = verifyServiceSid;
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

  public TwilioVerificationTextConfiguration getDefaultClientVerificationTexts() {
    return defaultClientVerificationTexts;
  }

  @VisibleForTesting
  public void setDefaultClientVerificationTexts(TwilioVerificationTextConfiguration defaultClientVerificationTexts) {
    this.defaultClientVerificationTexts = defaultClientVerificationTexts;
  }


  public Map<String,TwilioVerificationTextConfiguration> getRegionalClientVerificationTexts() {
    return regionalClientVerificationTexts;
  }

  @VisibleForTesting
  public void setRegionalClientVerificationTexts(final Map<String,TwilioVerificationTextConfiguration> regionalClientVerificationTexts) {
    this.regionalClientVerificationTexts = regionalClientVerificationTexts;
  }

  public String getAndroidAppHash() {
    return androidAppHash;
  }

  public void setAndroidAppHash(String androidAppHash) {
    this.androidAppHash = androidAppHash;
  }

  public void setVerifyServiceFriendlyName(String serviceFriendlyName) {
    this.verifyServiceFriendlyName = serviceFriendlyName;
  }

  public String getVerifyServiceFriendlyName() {
    return verifyServiceFriendlyName;
  }
}
