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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.NexmoConfiguration;
import org.whispersystems.textsecuregcm.util.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

import static com.codahale.metrics.MetricRegistry.name;

public class NexmoSmsSender {

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          smsMeter       = metricRegistry.meter(name(getClass(), "sms", "delivered"));
  private final Meter          voxMeter       = metricRegistry.meter(name(getClass(), "vox", "delivered"));
  private final Logger         logger         = LoggerFactory.getLogger(NexmoSmsSender.class);

  private static final String NEXMO_SMS_URL =
      "https://rest.nexmo.com/sms/json?api_key=%s&api_secret=%s&from=%s&to=%s&text=%s";

  private static final String NEXMO_VOX_URL =
      "https://rest.nexmo.com/tts/json?api_key=%s&api_secret=%s&to=%s&text=%s";

  private final String apiKey;
  private final String apiSecret;
  private final String number;

  public NexmoSmsSender(NexmoConfiguration config) {
    this.apiKey    = config.getApiKey();
    this.apiSecret = config.getApiSecret();
    this.number    = config.getNumber();
  }

  public void deliverSmsVerification(String destination, String verificationCode) throws IOException {
    URL url = new URL(String.format(NEXMO_SMS_URL, apiKey, apiSecret, number, destination,
                                    URLEncoder.encode(SmsSender.SMS_VERIFICATION_TEXT + verificationCode, "UTF-8")));

    URLConnection connection = url.openConnection();
    connection.setDoInput(true);
    connection.connect();

    BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
    while (reader.readLine() != null) {}
    reader.close();
    smsMeter.mark();
  }

  public void deliverVoxVerification(String destination, String message) throws IOException {
    URL url = new URL(String.format(NEXMO_VOX_URL, apiKey, apiSecret, destination,
                                    URLEncoder.encode(SmsSender.VOX_VERIFICATION_TEXT + message, "UTF-8")));

    URLConnection connection = url.openConnection();
    connection.setDoInput(true);
    connection.connect();

    BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
    String line;
    while ((line = reader.readLine()) != null) {
      logger.debug(line);
    }
    reader.close();
    voxMeter.mark();
  }


}
