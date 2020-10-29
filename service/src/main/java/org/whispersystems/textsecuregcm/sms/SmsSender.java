/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.sms;


import java.util.Optional;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SmsSender {
  private final TwilioSmsSender twilioSender;

  public SmsSender(TwilioSmsSender twilioSender) {
    this.twilioSender = twilioSender;
  }

  public void deliverSmsVerification(String destination, Optional<String> clientType, String verificationCode) {
    // Fix up mexico numbers to 'mobile' format just for SMS delivery.
    if (destination.startsWith("+52") && !destination.startsWith("+521")) {
      destination = "+521" + destination.substring(3);
    }

    twilioSender.deliverSmsVerification(destination, clientType, verificationCode);
  }

  public void deliverVoxVerification(String destination, String verificationCode, Optional<String> locale) {
    twilioSender.deliverVoxVerification(destination, verificationCode, locale);
  }
}
