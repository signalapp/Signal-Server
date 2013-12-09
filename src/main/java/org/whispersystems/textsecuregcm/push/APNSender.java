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

import com.google.common.base.Optional;
import com.notnoop.apns.APNS;
import com.notnoop.apns.ApnsService;
import com.notnoop.exceptions.NetworkIOException;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import org.bouncycastle.openssl.PEMReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.EncryptedOutgoingMessage;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

public class APNSender {

  private final Meter  success = Metrics.newMeter(APNSender.class, "sent", "success", TimeUnit.MINUTES);
  private final Meter  failure = Metrics.newMeter(APNSender.class, "sent", "failure", TimeUnit.MINUTES);
  private final Logger logger  = LoggerFactory.getLogger(APNSender.class);

  private static final String MESSAGE_BODY = "m";

  private final Optional<ApnsService> apnService;

  public APNSender(String apnCertificate, String apnKey)
      throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException
  {
    if (!Util.isEmpty(apnCertificate) && !Util.isEmpty(apnKey)) {
      byte[] keyStore = initializeKeyStore(apnCertificate, apnKey);
      this.apnService = Optional.of(APNS.newService()
                                        .withCert(new ByteArrayInputStream(keyStore), "insecure")
                                        .withSandboxDestination().build());
    } else {
      this.apnService = Optional.absent();
    }
  }

  public void sendMessage(String registrationId, EncryptedOutgoingMessage message)
      throws IOException
  {
    try {
      if (!apnService.isPresent()) {
        failure.mark();
        throw new IOException("APN access not configured!");
      }

      String payload = APNS.newPayload()
                           .alertBody("Message!")
                           .customField(MESSAGE_BODY, message.serialize())
                           .build();

      logger.debug("APN Payload: " + payload);

      apnService.get().push(registrationId, payload);
      success.mark();
    } catch (MalformedURLException mue) {
      throw new AssertionError(mue);
    } catch (NetworkIOException nioe) {
      logger.warn("Network Error", nioe);
      failure.mark();
      throw new IOException("Error sending APN");
    }
  }

  private static byte[] initializeKeyStore(String pemCertificate, String pemKey)
      throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException
  {
    PEMReader       reader           = new PEMReader(new InputStreamReader(new ByteArrayInputStream(pemCertificate.getBytes())));
    X509Certificate certificate      = (X509Certificate) reader.readObject();
    Certificate[]   certificateChain = {certificate};

    reader            = new PEMReader(new InputStreamReader(new ByteArrayInputStream(pemKey.getBytes())));
    KeyPair keyPair = (KeyPair) reader.readObject();

    KeyStore keyStore = KeyStore.getInstance("pkcs12");
    keyStore.load(null);
    keyStore.setEntry("apn",
                      new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), certificateChain),
                      new KeyStore.PasswordProtection("insecure".toCharArray()));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    keyStore.store(baos, "insecure".toCharArray());

    return baos.toByteArray();
  }
}
