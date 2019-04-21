/*
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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.TwilioConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.http.FormDataBodyPublisher;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.ExecutorUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.codahale.metrics.MetricRegistry.name;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class TwilioSmsSender {

  private static final Logger         logger         = LoggerFactory.getLogger(TwilioSmsSender.class);

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          smsMeter       = metricRegistry.meter(name(getClass(), "sms", "delivered"));
  private final Meter          voxMeter       = metricRegistry.meter(name(getClass(), "vox", "delivered"));
  private final Meter          priceMeter     = metricRegistry.meter(name(getClass(), "price"));

  private final String            accountId;
  private final String            accountToken;
  private final ArrayList<String> numbers;
  private final String            messagingServicesId;
  private final String            localDomain;
  private final Random            random;

  private final FaultTolerantHttpClient httpClient;
  private final URI                     smsUri;
  private final URI                     voxUri;

  @VisibleForTesting
  public TwilioSmsSender(String baseUri, TwilioConfiguration twilioConfiguration) {
    Executor executor = ExecutorUtils.newFixedThreadBoundedQueueExecutor(10, 100);

    this.accountId           = twilioConfiguration.getAccountId();
    this.accountToken        = twilioConfiguration.getAccountToken();
    this.numbers             = new ArrayList<>(twilioConfiguration.getNumbers());
    this.localDomain         = twilioConfiguration.getLocalDomain();
    this.messagingServicesId = twilioConfiguration.getMessagingServicesId();
    this.random              = new Random(System.currentTimeMillis());
    this.smsUri              = URI.create(baseUri + "/2010-04-01/Accounts/" + accountId + "/Messages.json");
    this.voxUri              = URI.create(baseUri + "/2010-04-01/Accounts/" + accountId + "/Calls.json"   );
    this.httpClient          = FaultTolerantHttpClient.newBuilder()
                                                      .withCircuitBreaker(twilioConfiguration.getCircuitBreaker())
                                                      .withRetry(twilioConfiguration.getRetry())
                                                      .withVersion(HttpClient.Version.HTTP_2)
                                                      .withConnectTimeout(Duration.ofSeconds(10))
                                                      .withRedirect(HttpClient.Redirect.NEVER)
                                                      .withExecutor(executor)
                                                      .withName("twilio")
                                                      .build();
  }

  public TwilioSmsSender(TwilioConfiguration twilioConfiguration) {
      this("https://api.twilio.com", twilioConfiguration);
  }

  public CompletableFuture<Boolean> deliverSmsVerification(String destination, Optional<String> clientType, String verificationCode) {
    Map<String, String> requestParameters = new HashMap<>();
    requestParameters.put("To", destination);

    if (Util.isEmpty(messagingServicesId)) {
      requestParameters.put("From", getRandom(random, numbers));
    } else {
      requestParameters.put("MessagingServiceSid", messagingServicesId);
    }

    if ("ios".equals(clientType.orElse(null))) {
      requestParameters.put("Body", String.format(SmsSender.SMS_IOS_VERIFICATION_TEXT, verificationCode, verificationCode));
    } else if ("android-ng".equals(clientType.orElse(null))) {
      requestParameters.put("Body", String.format(SmsSender.SMS_ANDROID_NG_VERIFICATION_TEXT, verificationCode));
    } else {
      requestParameters.put("Body", String.format(SmsSender.SMS_VERIFICATION_TEXT, verificationCode));
    }

    HttpRequest request = HttpRequest.newBuilder()
                                     .uri(smsUri)
                                     .POST(FormDataBodyPublisher.of(requestParameters))
                                     .header("Content-Type", "application/x-www-form-urlencoded")
                                     .header("Authorization", "Basic " + Base64.encodeBytes((accountId + ":" + accountToken).getBytes()))
                                     .build();

    smsMeter.mark();

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                     .thenApply(this::parseResponse)
                     .handle(this::processResponse);
  }

  public CompletableFuture<Boolean> deliverVoxVerification(String destination, String verificationCode, Optional<String> locale) {
    String url = "https://" + localDomain + "/v1/voice/description/" + verificationCode;

    if (locale.isPresent()) {
      url += "?l=" + locale.get();
    }

    Map<String, String> requestParameters = new HashMap<>();
    requestParameters.put("Url", url);
    requestParameters.put("To", destination);
    requestParameters.put("From", getRandom(random, numbers));

    HttpRequest request = HttpRequest.newBuilder()
                                     .uri(voxUri)
                                     .POST(FormDataBodyPublisher.of(requestParameters))
                                     .header("Content-Type", "application/x-www-form-urlencoded")
                                     .header("Authorization", "Basic " + Base64.encodeBytes((accountId + ":" + accountToken).getBytes()))
                                     .build();

    voxMeter.mark();

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                     .thenApply(this::parseResponse)
                     .handle(this::processResponse);
  }

  private String getRandom(Random random, ArrayList<String> elements) {
    return elements.get(random.nextInt(elements.size()));
  }

  private boolean processResponse(TwilioResponse response, Throwable throwable) {
    if (response != null && response.isSuccess()) {
      priceMeter.mark((long)(response.successResponse.price * 1000));
      return true;
    } else if (response != null && response.isFailure()) {
      logger.info("Twilio request failed: " + response.failureResponse.status + ", " + response.failureResponse.message);
      return false;
    } else if (throwable != null) {
      logger.info("Twilio request failed", throwable);
      return false;
    } else {
      logger.warn("No response or throwable!");
      return false;
    }
      }

  private TwilioResponse parseResponse(HttpResponse<String> response) {
    ObjectMapper mapper = SystemMapper.getMapper();

    if (response.statusCode() >= 200 && response.statusCode() < 300) {
      if ("application/json".equals(response.headers().firstValue("Content-Type").orElse(null))) {
        return new TwilioResponse(TwilioResponse.TwilioSuccessResponse.fromBody(mapper, response.body()));
      } else {
        return new TwilioResponse(new TwilioResponse.TwilioSuccessResponse());
      }
    }

    if ("application/json".equals(response.headers().firstValue("Content-Type").orElse(null))) {
      return new TwilioResponse(TwilioResponse.TwilioFailureResponse.fromBody(mapper, response.body()));
    } else {
      return new TwilioResponse(new TwilioResponse.TwilioFailureResponse());
    }
  }

  public static class TwilioResponse {

    private TwilioSuccessResponse successResponse;
    private TwilioFailureResponse failureResponse;

    TwilioResponse(TwilioSuccessResponse successResponse) {
      this.successResponse = successResponse;
    }

    TwilioResponse(TwilioFailureResponse failureResponse) {
      this.failureResponse = failureResponse;
    }

    boolean isSuccess() {
      return successResponse != null;
    }

    boolean isFailure() {
      return failureResponse != null;
    }

    private static class TwilioSuccessResponse {
      @JsonProperty
      private double price;

      static TwilioSuccessResponse fromBody(ObjectMapper mapper, String body) {
        try {
          return mapper.readValue(body, TwilioSuccessResponse.class);
        } catch (IOException e) {
          logger.warn("Error parsing twilio success response: " + e);
          return new TwilioSuccessResponse();
        }
      }
    }

    private static class TwilioFailureResponse {
      @JsonProperty
      private int status;

      @JsonProperty
      private String message;

      static TwilioFailureResponse fromBody(ObjectMapper mapper, String body) {
        try {
          return mapper.readValue(body, TwilioFailureResponse.class);
        } catch (IOException e) {
          logger.warn("Error parsing twilio success response: " + e);
          return new TwilioFailureResponse();
        }
      }
    }
  }
}
