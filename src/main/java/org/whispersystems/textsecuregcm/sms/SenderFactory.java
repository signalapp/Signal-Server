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


import com.google.common.base.Optional;
import org.whispersystems.textsecuregcm.configuration.NexmoConfiguration;
import org.whispersystems.textsecuregcm.configuration.TwilioConfiguration;

import java.io.IOException;

public class SenderFactory {

  private final TwilioSmsSender          twilioSender;
  private final Optional<NexmoSmsSender> nexmoSender;

  public SenderFactory(TwilioConfiguration twilioConfig, NexmoConfiguration nexmoConfig) {
    this.twilioSender = new TwilioSmsSender(twilioConfig);

    if (nexmoConfig != null) {
      this.nexmoSender = Optional.of(new NexmoSmsSender(nexmoConfig));
    } else {
      this.nexmoSender = Optional.absent();
    }
  }

  public SmsSender getSmsSender(String number) {
    if (nexmoSender.isPresent() && !isTwilioDestination(number)) {
      return nexmoSender.get();
    } else {
      return twilioSender;
    }
  }

  public VoxSender getVoxSender(String number) {
    if (nexmoSender.isPresent()) {
      return nexmoSender.get();
    } else {
      return twilioSender;
    }
  }

  private boolean isTwilioDestination(String number) {
    return number.length() == 12 && number.startsWith("+1");
  }

  public interface SmsSender {
    public static final String VERIFICATION_TEXT = "Your TextSecure verification code: ";
    public void deliverSmsVerification(String destination, String verificationCode) throws IOException;
  }

  public interface VoxSender {
    public static final String VERIFICATION_TEXT = "Your TextSecure verification code is: ";
    public void deliverVoxVerification(String destination, String verificationCode) throws IOException;
  }
}
