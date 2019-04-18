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
package org.whispersystems.textsecuregcm.sms;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SmsSender {

  static final String SMS_IOS_VERIFICATION_TEXT        = "Your Signal verification code: %s\n\nOr tap: sgnl://verify/%s";
  static final String SMS_ANDROID_NG_VERIFICATION_TEXT = "<#> Your Signal verification code: %s\n\ndoDiFGKPO1r";
  static final String SMS_VERIFICATION_TEXT            = "Your Signal verification code: %s";

  private final TwilioSmsSender twilioSender;

  public SmsSender(TwilioSmsSender twilioSender)
  {
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
