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
package org.whispersystems.textsecuregcm.push;

import com.google.android.gcm.server.Constants;
import com.google.android.gcm.server.Message;
import com.google.android.gcm.server.Result;
import com.google.android.gcm.server.Sender;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import org.whispersystems.textsecuregcm.controllers.NoSuchUserException;
import org.whispersystems.textsecuregcm.entities.EncryptedOutgoingMessage;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class GCMSender {

  private final Meter success = Metrics.newMeter(GCMSender.class, "sent", "success", TimeUnit.MINUTES);
  private final Meter failure = Metrics.newMeter(GCMSender.class, "sent", "failure", TimeUnit.MINUTES);

  private final Sender sender;

  public GCMSender(String apiKey) {
    this.sender = new Sender(apiKey);
  }

  public String sendMessage(String gcmRegistrationId, EncryptedOutgoingMessage outgoingMessage)
      throws IOException, NoSuchUserException
  {
    Message gcmMessage = new Message.Builder().addData("type", "message")
                                              .addData("message", outgoingMessage.serialize())
                                              .build();

    Result  result = sender.send(gcmMessage, gcmRegistrationId, 5);

    if (result.getMessageId() != null) {
      success.mark();
      return result.getCanonicalRegistrationId();
    } else {
      failure.mark();
      if (result.getErrorCodeName().equals(Constants.ERROR_NOT_REGISTERED)) {
        throw new NoSuchUserException("User no longer registered with GCM.");
      } else {
        throw new IOException("GCM Failed: " + result.getErrorCodeName());
      }
    }
  }
}
