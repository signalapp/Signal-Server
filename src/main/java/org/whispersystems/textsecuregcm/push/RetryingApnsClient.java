package org.whispersystems.textsecuregcm.push;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.nurkiewicz.asyncretry.AsyncRetryExecutor;
import com.nurkiewicz.asyncretry.RetryContext;
import com.nurkiewicz.asyncretry.RetryExecutor;
import com.nurkiewicz.asyncretry.function.RetryCallable;
import com.relayrides.pushy.apns.ApnsClient;
import com.relayrides.pushy.apns.ApnsClientBuilder;
import com.relayrides.pushy.apns.ApnsServerException;
import com.relayrides.pushy.apns.ClientNotConnectedException;
import com.relayrides.pushy.apns.DeliveryPriority;
import com.relayrides.pushy.apns.PushNotificationResponse;
import com.relayrides.pushy.apns.metrics.dropwizard.DropwizardApnsClientMetricsListener;
import com.relayrides.pushy.apns.util.SimpleApnsPushNotification;

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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.codahale.metrics.MetricRegistry.name;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public class RetryingApnsClient {

  private static final Logger logger = LoggerFactory.getLogger(RetryingApnsClient.class);

  private final ApnsClient    apnsClient;
  private final RetryExecutor retryExecutor;

  RetryingApnsClient(String apnCertificate, String apnKey, int retryCount)
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
                                             .build();
    this.retryExecutor = initializeExecutor(retryCount);
  }

  @VisibleForTesting
  public RetryingApnsClient(ApnsClient apnsClient, int retryCount) {
    this.apnsClient    = apnsClient;
    this.retryExecutor = initializeExecutor(retryCount);
  }

  private static RetryExecutor initializeExecutor(int retryCount) {
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    return new AsyncRetryExecutor(executorService).retryOn(ClientNotConnectedException.class)
                                                  .retryOn(InterruptedException.class)
                                                  .retryOn(ApnsServerException.class)
                                                  .withExponentialBackoff(100, 2.0)
                                                  .withUniformJitter()
                                                  .withMaxDelay(4000)
                                                  .withMaxRetries(retryCount);
  }

  ListenableFuture<ApnResult> send(final String apnId, final String topic, final String payload, final Date expiration) {
    return this.retryExecutor.getFutureWithRetry(new RetryCallable<ListenableFuture<ApnResult>>() {
      @Override
      public ListenableFuture<ApnResult> call(RetryContext context) throws Exception {
        SettableFuture<ApnResult>  result       = SettableFuture.create();
        SimpleApnsPushNotification notification = new SimpleApnsPushNotification(apnId, topic, payload, expiration, DeliveryPriority.IMMEDIATE);
        
        apnsClient.sendNotification(notification).addListener(new ResponseHandler(apnsClient, result));

        return result;
      }
    });
  }

  void connect(boolean sandbox) {
    apnsClient.connect(sandbox ? ApnsClient.DEVELOPMENT_APNS_HOST : ApnsClient.PRODUCTION_APNS_HOST).awaitUninterruptibly();
  }

  void disconnect() {
    apnsClient.disconnect();
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

    private final ApnsClient                client;
    private final SettableFuture<ApnResult> future;

    private ResponseHandler(ApnsClient client, SettableFuture<ApnResult> future) {
      this.client = client;
      this.future = future;
    }

    @Override
    public void operationComplete(io.netty.util.concurrent.Future<PushNotificationResponse<SimpleApnsPushNotification>> result) {
      try {
        PushNotificationResponse<SimpleApnsPushNotification> response = result.get();

        if (response.isAccepted()) {
          future.set(new ApnResult(ApnResult.Status.SUCCESS, null));
        } else if ("Unregistered".equals(response.getRejectionReason())) {
          future.set(new ApnResult(ApnResult.Status.NO_SUCH_USER, response.getRejectionReason()));
        } else {
          logger.warn("Got APN failure: " + response.getRejectionReason());
          future.set(new ApnResult(ApnResult.Status.GENERIC_FAILURE, response.getRejectionReason()));
        }

      } catch (InterruptedException e) {
        future.setException(e);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof ClientNotConnectedException) setDisconnected(e.getCause());
        else                                                     future.setException(e.getCause());
      }
    }

    private void setDisconnected(final Throwable t) {
      logger.warn("Client disconnected, waiting for reconnect...", t);
      client.getReconnectionFuture().addListener(new GenericFutureListener<Future<Void>>() {
        @Override
        public void operationComplete(Future<Void> complete) {
          logger.warn("Client reconnected...");
          future.setException(t);
        }
      });
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
