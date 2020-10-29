/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import com.google.common.annotations.VisibleForTesting;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;

public class TwilioConfiguration {

  @NotEmpty
  private String accountId;

  @NotEmpty
  private String accountToken;

  @NotNull
  private List<String> numbers;

  @NotEmpty
  private String localDomain;

  private String messagingServiceSid;
  private String nanpaMessagingServiceSid;

  @NotNull
  @Valid
  private CircuitBreakerConfiguration circuitBreaker = new CircuitBreakerConfiguration();

  @NotNull
  @Valid
  private RetryConfiguration retry = new RetryConfiguration();

  @NotNull
  @Valid
  private TwilioSenderIdConfiguration senderId = new TwilioSenderIdConfiguration();

  @NotEmpty
  private String iosVerificationText;

  @NotEmpty
  private String androidNgVerificationText;

  @NotEmpty
  private String android202001VerificationText;

  @NotEmpty
  private String genericVerificationText;

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

  public TwilioSenderIdConfiguration getSenderId() {
    return senderId;
  }

  @VisibleForTesting
  public void setSenderId(TwilioSenderIdConfiguration senderId) {
    this.senderId = senderId;
  }

  public String getIosVerificationText() {
    return iosVerificationText;
  }

  @VisibleForTesting
  public void setIosVerificationText(String iosVerificationText) {
    this.iosVerificationText = iosVerificationText;
  }

  public String getAndroidNgVerificationText() {
    return androidNgVerificationText;
  }

  @VisibleForTesting
  public void setAndroidNgVerificationText(String androidNgVerificationText) {
    this.androidNgVerificationText = androidNgVerificationText;
  }

  public String getAndroid202001VerificationText() {
    return android202001VerificationText;
  }

  @VisibleForTesting
  public void setAndroid202001VerificationText(String android202001VerificationText) {
    this.android202001VerificationText = android202001VerificationText;
  }

  public String getGenericVerificationText() {
    return genericVerificationText;
  }

  @VisibleForTesting
  public void setGenericVerificationText(String genericVerificationText) {
    this.genericVerificationText = genericVerificationText;
  }
}
