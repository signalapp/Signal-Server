package org.whispersystems.textsecuregcm.push;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.turo.pushy.apns.ApnsClient;
import com.turo.pushy.apns.ApnsClientBuilder;
import com.turo.pushy.apns.DeliveryPriority;
import com.turo.pushy.apns.PushNotificationResponse;
import com.turo.pushy.apns.metrics.dropwizard.DropwizardApnsClientMetricsListener;
import com.turo.pushy.apns.util.SimpleApnsPushNotification;
import org.bouncycastle.openssl.PEMReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Constants;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.codahale.metrics.MetricRegistry.name;
import io.netty.util.concurrent.GenericFutureListener;

public class RetryingApnsClient {

  private static final Logger logger = LoggerFactory.getLogger(RetryingApnsClient.class);

  private final ApnsClient apnsClient;

  RetryingApnsClient(String apnCertificate, String apnKey, boolean sandbox)
      throws IOException
  {
    MetricRegistry                      metricRegistry  = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
    DropwizardApnsClientMetricsListener metricsListener = new DropwizardApnsClientMetricsListener();

    for (Map.Entry<String, Metric> entry : metricsListener.getMetrics().entrySet()) {
      metricRegistry.register(name(getClass(), entry.getKey()), entry.getValue());
    }

    this.apnsClient = new ApnsClientBuilder().setClientCredentials(initializeCertificate(apnCertificate),
                                                                   initializePrivateKey(apnKey), null)
                                             .setMetricsListener(metricsListener)
                                             .setApnsServer(sandbox ? ApnsClientBuilder.DEVELOPMENT_APNS_HOST : ApnsClientBuilder.PRODUCTION_APNS_HOST)
                                             .build();
  }

  @VisibleForTesting
  public RetryingApnsClient(ApnsClient apnsClient) {
    this.apnsClient = apnsClient;
  }

  ListenableFuture<ApnResult> send(final String apnId, final String topic, final String payload, final Date expiration) {
    SettableFuture<ApnResult>  result       = SettableFuture.create();
    SimpleApnsPushNotification notification = new SimpleApnsPushNotification(apnId, topic, payload, expiration, DeliveryPriority.IMMEDIATE);
        
    apnsClient.sendNotification(notification).addListener(new ResponseHandler(result));

    return result;
  }

  void disconnect() {
    apnsClient.close();
  }

  private static X509Certificate initializeCertificate(String pemCertificate) throws IOException {
    PEMReader reader = new PEMReader(new InputStreamReader(new ByteArrayInputStream(pemCertificate.getBytes())));
    return (X509Certificate) reader.readObject();
  }

  private static PrivateKey initializePrivateKey(String pemKey) throws IOException {
    PEMReader reader = new PEMReader(new InputStreamReader(new ByteArrayInputStream(pemKey.getBytes())));
    return ((KeyPair) reader.readObject()).getPrivate();
  }

  private static final class ResponseHandler implements GenericFutureListener<io.netty.util.concurrent.Future<PushNotificationResponse<SimpleApnsPushNotification>>> {

    private final SettableFuture<ApnResult> future;

    private ResponseHandler(SettableFuture<ApnResult> future) {
      this.future = future;
    }

    @Override
    public void operationComplete(io.netty.util.concurrent.Future<PushNotificationResponse<SimpleApnsPushNotification>> result) {
      try {
        PushNotificationResponse<SimpleApnsPushNotification> response = result.get();

        if (response.isAccepted()) {
          future.set(new ApnResult(ApnResult.Status.SUCCESS, null));
        } else if ("Unregistered".equals(response.getRejectionReason()) ||
                   "BadDeviceToken".equals(response.getRejectionReason()))
        {
          future.set(new ApnResult(ApnResult.Status.NO_SUCH_USER, response.getRejectionReason()));
        } else {
          logger.warn("Got APN failure: " + response.getRejectionReason());
          future.set(new ApnResult(ApnResult.Status.GENERIC_FAILURE, response.getRejectionReason()));
        }

      } catch (InterruptedException e) {
        logger.warn("Interrupted exception", e);
        future.setException(e);
      } catch (ExecutionException e) {
        logger.warn("Execution exception", e);
        future.setException(e.getCause());
      }
    }
  }

  public static class ApnResult {
    public enum Status {
      SUCCESS, NO_SUCH_USER, GENERIC_FAILURE
    }

    private final Status status;
    private final String reason;

    ApnResult(Status status, String reason) {
      this.status = status;
      this.reason = reason;
    }

    public Status getStatus() {
      return status;
    }

    public String getReason() {
      return reason;
    }
  }

}
