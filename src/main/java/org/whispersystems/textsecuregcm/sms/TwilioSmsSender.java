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

import com.sun.jersey.core.util.Base64;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import org.whispersystems.textsecuregcm.configuration.TwilioConfiguration;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TwilioSmsSender implements SenderFactory.SmsSender {

  private final Meter smsMeter = Metrics.newMeter(TwilioSmsSender.class, "sms", "delivered", TimeUnit.MINUTES);

  private static final String TWILIO_URL = "https://api.twilio.com/2010-04-01/Accounts/%s/SMS/Messages";

  private final String accountId;
  private final String accountToken;
  private final String number;

  public TwilioSmsSender(TwilioConfiguration config) {
    this.accountId    = config.getAccountId();
    this.accountToken = config.getAccountToken();
    this.number       = config.getNumber();
  }

  @Override
  public void deliverSmsVerification(String destination, String verificationCode) throws IOException {
    URL url                      = new URL(String.format(TWILIO_URL, accountId));
    URLConnection connection     = url.openConnection();
    connection.setDoOutput(true);
    connection.setRequestProperty("Authorization", getTwilioAuthorizationHeader());
    connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

    Map<String, String> formData = new HashMap<>();
    formData.put("From", number);
    formData.put("To", destination);
    formData.put("Body", VERIFICATION_TEXT + verificationCode);

    OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
    writer.write(Util.encodeFormParams(formData));
    writer.flush();

    BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
    while (reader.readLine() != null) {}
    writer.close();
    reader.close();

    smsMeter.mark();
  }

  private String getTwilioAuthorizationHeader() {
    String encoded = new String(Base64.encode(String.format("%s:%s", accountId, accountToken)));
    return "Basic " + encoded.replace("\n", "");
  }

}
