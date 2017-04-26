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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.notnoop.apns.APNS;
import com.notnoop.apns.ApnsService;
import com.notnoop.apns.ApnsServiceBuilder;
import com.notnoop.exceptions.NetworkIOException;
import org.bouncycastle.openssl.PEMReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.ApnConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.dropwizard.lifecycle.Managed;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class APNSender implements Managed {

  private final Logger logger = LoggerFactory.getLogger(APNSender.class);

  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

  private final AccountsManager accountsManager;
  private final JedisPool       jedisPool;

  private final String    pushCertificate;
  private final String    pushKey;

  private final String    voipCertificate;
  private final String    voipKey;

  private final boolean   feedbackEnabled;
  private final boolean   sandbox;

  private ApnsService pushApnService;
  private ApnsService voipApnService;

  public APNSender(AccountsManager accountsManager,
                   JedisPool jedisPool,
                   ApnConfiguration configuration)
  {
    this.accountsManager = accountsManager;
    this.jedisPool       = jedisPool;
    this.pushCertificate = configuration.getPushCertificate();
    this.pushKey         = configuration.getPushKey();
    this.voipCertificate = configuration.getVoipCertificate();
    this.voipKey         = configuration.getVoipKey();
    this.feedbackEnabled = configuration.isFeedbackEnabled();
    this.sandbox         = configuration.isSandboxEnabled();
  }

  @VisibleForTesting
  public APNSender(AccountsManager accountsManager, JedisPool jedisPool,
                   ApnsService pushApnService, ApnsService voipApnService,
                   boolean feedbackEnabled, boolean sandbox)
  {
    this.accountsManager = accountsManager;
    this.jedisPool       = jedisPool;
    this.pushApnService  = pushApnService;
    this.voipApnService  = voipApnService;
    this.feedbackEnabled = feedbackEnabled;
    this.sandbox         = sandbox;
    this.pushCertificate = null;
    this.pushKey         = null;
    this.voipCertificate = null;
    this.voipKey         = null;
  }

  public void sendMessage(ApnMessage message)
      throws TransientPushFailureException
  {
    try {
      redisSet(message.getApnId(), message.getNumber(), message.getDeviceId());

      if (message.isVoip()) {
        voipApnService.push(message.getApnId(), message.getMessage(), new Date(message.getExpirationTime()));
      } else {
        pushApnService.push(message.getApnId(), message.getMessage(), new Date(message.getExpirationTime()));
      }
    } catch (NetworkIOException nioe) {
      logger.warn("Network Error", nioe);
      throw new TransientPushFailureException(nioe);
    }
  }

  private static byte[] initializeKeyStore(String pemCertificate, String pemKey)
      throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException
  {
    PEMReader       reader           = new PEMReader(new InputStreamReader(new ByteArrayInputStream(pemCertificate.getBytes())));
    X509Certificate certificate      = (X509Certificate) reader.readObject();
    Certificate[]   certificateChain = {certificate};

    reader = new PEMReader(new InputStreamReader(new ByteArrayInputStream(pemKey.getBytes())));
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

  @Override
  public void start() throws Exception {
    byte[] pushKeyStore = initializeKeyStore(pushCertificate, pushKey);
    byte[] voipKeyStore = initializeKeyStore(voipCertificate, voipKey);

    ApnsServiceBuilder pushApnServiceBuilder = APNS.newService()
                                                   .withCert(new ByteArrayInputStream(pushKeyStore), "insecure")
                                                   .asQueued();


    ApnsServiceBuilder voipApnServiceBuilder = APNS.newService()
                                                   .withCert(new ByteArrayInputStream(voipKeyStore), "insecure")
                                                   .asQueued();


    if (sandbox) {
      this.pushApnService = pushApnServiceBuilder.withSandboxDestination().build();
      this.voipApnService = voipApnServiceBuilder.withSandboxDestination().build();
    } else {
      this.pushApnService = pushApnServiceBuilder.withProductionDestination().build();
      this.voipApnService = voipApnServiceBuilder.withProductionDestination().build();
    }

    if (feedbackEnabled) {
      this.executor.scheduleAtFixedRate(new FeedbackRunnable(), 0, 1, TimeUnit.HOURS);
    }
  }

  @Override
  public void stop() throws Exception {
    pushApnService.stop();
    voipApnService.stop();
  }

  private void redisSet(String registrationId, String number, int deviceId) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.set("APN-" + registrationId.toLowerCase(), number + "." + deviceId);
      jedis.expire("APN-" + registrationId.toLowerCase(), (int) TimeUnit.HOURS.toSeconds(1));
    }
  }

  private Optional<String> redisGet(String registrationId) {
    try (Jedis jedis = jedisPool.getResource()) {
      String number = jedis.get("APN-" + registrationId.toLowerCase());
      return Optional.fromNullable(number);
    }
  }

  @VisibleForTesting
  public void checkFeedback() {
    new FeedbackRunnable().run();
  }

  private class FeedbackRunnable implements Runnable {

    @Override
    public void run() {
      try {
        Map<String, Date> inactiveDevices = pushApnService.getInactiveDevices();
        inactiveDevices.putAll(voipApnService.getInactiveDevices());

        for (String registrationId : inactiveDevices.keySet()) {
          Optional<String> device = redisGet(registrationId);

          if (device.isPresent()) {
            logger.warn("Got APN unregistered notice!");
            String[] parts    = device.get().split("\\.", 2);

            if (parts.length == 2) {
              String number    = parts[0];
              int    deviceId  = Integer.parseInt(parts[1]);
              long   timestamp = inactiveDevices.get(registrationId).getTime();

              handleApnUnregistered(registrationId, number, deviceId, timestamp);
            } else {
              logger.warn("APN unregister event for device with no parts: " + device.get());
            }
          } else {
            logger.warn("APN unregister event received for uncached ID: " + registrationId);
          }
        }
      } catch (Throwable t) {
        logger.warn("Exception during feedback", t);
      }
    }

    private void handleApnUnregistered(String registrationId, String number, int deviceId, long timestamp) {
      logger.info("Got APN Unregistered: " + number + "," + deviceId);

      Optional<Account> account = accountsManager.get(number);

      if (account.isPresent()) {
        Optional<Device> device = account.get().getDevice(deviceId);

        if (device.isPresent()) {
          if (registrationId.equals(device.get().getApnId())) {
            logger.info("APN Unregister APN ID matches!");
            if (device.get().getPushTimestamp() == 0 ||
                timestamp > device.get().getPushTimestamp())
            {
              logger.info("APN Unregister timestamp matches!");
              device.get().setApnId(null);
              accountsManager.update(account.get());
            }
          }
        }
      }
    }
  }
}
