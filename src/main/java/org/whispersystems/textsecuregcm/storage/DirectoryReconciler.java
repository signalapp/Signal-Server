/**
 * Copyright (C) 2018 Open WhisperSystems
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
package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import io.dropwizard.lifecycle.Managed;
import org.bouncycastle.openssl.PEMReader;
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.ContactDiscoveryConfiguration;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Hex;
import org.whispersystems.textsecuregcm.util.Util;
import redis.clients.jedis.Jedis;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Random;

import static com.codahale.metrics.MetricRegistry.name;

public class DirectoryReconciler implements Managed {

  private static final Logger logger = LoggerFactory.getLogger(DirectoryReconciler.class);

  private static final MetricRegistry metricRegistry       = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          readBucketTimer      = metricRegistry.timer(name(DirectoryReconciler.class, "readBucket"));
  private static final Timer          sendBucketTimer      = metricRegistry.timer(name(DirectoryReconciler.class, "sendBucket"));
  private static final Meter          sendBucketErrorMeter = metricRegistry.meter(name(DirectoryReconciler.class, "sendBucketError"));

  private static final String   WORKERS_KEY   = "directory_reconciliation_workers";
  private static final String   COUNTER_KEY   = "directory_reconciliation_counter";
  private static final long     WORKER_TTL_MS = 120_000L;
  private static final long     BUCKET_ORDER  = 11L;
  private static final long     BUCKET_COUNT  = 1L << BUCKET_ORDER;
  private static final double   JITTER_MAX    = 0.20;
  private static final String[] COUNTER_ARGS  = {
          "OVERFLOW", "WRAP",
          "INCRBY", "u32", "0", "1",
          };

  private final String              serverApiUrl;
  private final ReplicatedJedisPool jedisPool;
  private final AccountsManager     accountsManager;
  private final Client              client;

  private DirectoryReconciliationWorker directoryReconciliationWorker;

  public DirectoryReconciler(ContactDiscoveryConfiguration cdsConfig,
                             ReplicatedJedisPool jedisPool,
                             AccountsManager accountsManager)
  {
    this.serverApiUrl = cdsConfig.getServerApiUrl();
    this.jedisPool = jedisPool;
    this.accountsManager = accountsManager;

    SslConfigurator sslConfig = SslConfigurator.newInstance()
                                               .securityProtocol("TLSv1.2");
    try {
      sslConfig.trustStore(initializeKeyStore(cdsConfig.getServerApiCaCertificate()));
    } catch (CertificateException ex) {
      logger.error("error reading serverApiCaCertificate from contactDiscovery config", ex);
    }

    this.client = ClientBuilder.newBuilder()
                               .register(HttpAuthenticationFeature.basic("", cdsConfig.getServerApiToken().getBytes()))
                               .sslContext(sslConfig.createSSLContext())
                               .build();
  }

  private static KeyStore initializeKeyStore(String pemCaCertificate)
          throws CertificateException
  {
    try {
      PEMReader       reader      = new PEMReader(new InputStreamReader(new ByteArrayInputStream(pemCaCertificate.getBytes())));
      X509Certificate certificate = (X509Certificate) reader.readObject();

      if (certificate == null) {
        throw new CertificateException("No certificate found in parsing!");
      }

      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null);
      keyStore.setCertificateEntry("ca", certificate);
      return keyStore;
    } catch (IOException | KeyStoreException ex) {
      throw new CertificateException(ex.toString());
    } catch (NoSuchAlgorithmException ex) {
      throw new AssertionError(ex);
    }
  }

  @Override
  public void start() {
    this.directoryReconciliationWorker = new DirectoryReconciliationWorker(serverApiUrl, jedisPool, accountsManager, client);

    this.directoryReconciliationWorker.start();
  }

  @Override
  public void stop() {
    directoryReconciliationWorker.shutdown();
  }

  private static class DirectoryReconciliationWorker extends Thread {

    private final String              serverApiUrl;
    private final ReplicatedJedisPool jedisPool;
    private final AccountsManager     accountsManager;
    private final Client              client;

    private final String workerId;
    private final Random random;

    private boolean running  = true;
    private boolean finished = false;

    DirectoryReconciliationWorker(String serverApiUrl,
                                  ReplicatedJedisPool jedisPool,
                                  AccountsManager accountsManager,
                                  Client client)
    {
      super(DirectoryReconciliationWorker.class.getSimpleName());

      this.serverApiUrl = serverApiUrl;
      this.jedisPool = jedisPool;
      this.accountsManager = accountsManager;
      this.client = client;

      SecureRandom secureRandom  = new SecureRandom();
      byte[]       workerIdBytes = new byte[16];
      secureRandom.nextBytes(new byte[16]);
      this.workerId = Hex.toString(workerIdBytes);

      this.random = new Random(secureRandom.nextLong());
    }

    synchronized void shutdown() {
      running = false;
      while (!finished) {
        Util.wait(this);
      }
    }

    synchronized boolean sleepWhileRunning(long delay) {
      long start   = System.currentTimeMillis();
      long elapsed = 0;
      while (running && elapsed < delay) {
        try {
          wait(delay - elapsed);
        } catch (InterruptedException ex) {
        }
        elapsed = System.currentTimeMillis() - start;
      }
      return running;
    }

    private List<String> readBucket(long bucket) {
      Timer.Context timer = readBucketTimer.time();
      try {
        return accountsManager.getNumbersInBucket((1L << BUCKET_ORDER) - 1L, bucket);
      } finally {
        timer.stop();
      }
    }

    private void sendBucket(long bucket, List<String> numbers) {
      Timer.Context timer = sendBucketTimer.time();
      try {
        Response response = client.target(serverApiUrl)
                                  .path(String.format("/v1/directory/reconcile/%d/%d", BUCKET_COUNT, bucket))
                                  .request(MediaType.APPLICATION_JSON)
                                  .put(Entity.json(new DirectoryReconciliationRequest(numbers)));
        if (response.getStatus() != 200) {
          sendBucketErrorMeter.mark();
          logger.warn("http error " + response.getStatus());
        }
      } catch (ProcessingException ex) {
        sendBucketErrorMeter.mark();
        logger.warn("request error: ", ex);
      } finally {
        timer.stop();
      }
    }

    @Override
    public void run() {
      logger.info("Directory reconciliation worker %s started", workerId);

      try (Jedis jedis = jedisPool.getWriteResource()) {
        long nowMs = System.currentTimeMillis();
        jedis.zremrangeByScore(WORKERS_KEY, Double.NEGATIVE_INFINITY, (double) (nowMs - WORKER_TTL_MS));
      }

      long workIntervalMs = 0;
      long lastWorkTimeMs = 0;
      long lastPingTimeMs = 0;
      long randomJitterMs = 0;
      while (sleepWhileRunning(Math.min(lastWorkTimeMs + workIntervalMs + randomJitterMs,
                                        lastPingTimeMs + WORKER_TTL_MS / 2) - System.currentTimeMillis())) {
        long nowMs = System.currentTimeMillis();

        try (Jedis jedis = jedisPool.getWriteResource()) {
          lastPingTimeMs = nowMs;

          jedis.zadd(WORKERS_KEY, (double) nowMs, workerId);

          if (nowMs - lastWorkTimeMs > workIntervalMs + randomJitterMs) {
            lastWorkTimeMs = nowMs;

            long workerCount = jedis.zcount(WORKERS_KEY, (double) (nowMs - WORKER_TTL_MS), Double.POSITIVE_INFINITY);

            workIntervalMs = 86400_000L * workerCount / BUCKET_COUNT;
            randomJitterMs = (long) (random.nextDouble() * JITTER_MAX * workIntervalMs);

            long counter = jedis.bitfield(COUNTER_KEY, COUNTER_ARGS).get(0);
            long bucket  = counter % BUCKET_COUNT;

            List<String> numbers = readBucket(bucket);
            sendBucket(bucket, numbers);
          } else if (lastWorkTimeMs > nowMs) {
            lastWorkTimeMs = nowMs;
          }
        }
      }

      try (Jedis jedis = jedisPool.getWriteResource()) {
        jedis.zrem(WORKERS_KEY, workerId);
      }

      logger.info("Directory reconciliation worker %s shut down", workerId);

      synchronized (this) {
        finished = true;
        notifyAll();
      }
    }

  }

}
