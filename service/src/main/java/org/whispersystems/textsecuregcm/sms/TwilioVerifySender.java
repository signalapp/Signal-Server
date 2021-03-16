package org.whispersystems.textsecuregcm.sms;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Locale.LanguageRange;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.validation.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.TwilioConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.http.FormDataBodyPublisher;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
class TwilioVerifySender {

  private static final Logger logger = LoggerFactory.getLogger(TwilioVerifySender.class);

  static final Set<String> TWILIO_VERIFY_LANGUAGES = Set.of(
      "af",
      "ar",
      "ca",
      "zh",
      "zh-CN",
      "zh-HK",
      "hr",
      "cs",
      "da",
      "nl",
      "en",
      "en-GB",
      "fi",
      "fr",
      "de",
      "el",
      "he",
      "hi",
      "hu",
      "id",
      "it",
      "ja",
      "ko",
      "ms",
      "nb",
      "pl",
      "pt",
      "pt-BR",
      "ro",
      "ru",
      "es",
      "sv",
      "tl",
      "th",
      "tr",
      "vi");

  private final String accountId;
  private final String accountToken;

  private final URI verifyServiceUri;
  private final URI verifyApprovalBaseUri;
  private final String androidAppHash;
  private final FaultTolerantHttpClient httpClient;

  TwilioVerifySender(String baseUri, FaultTolerantHttpClient httpClient, TwilioConfiguration twilioConfiguration) {

    this.accountId = twilioConfiguration.getAccountId();
    this.accountToken = twilioConfiguration.getAccountToken();

    this.verifyServiceUri = URI
        .create(baseUri + "/v2/Services/" + twilioConfiguration.getVerifyServiceSid() + "/Verifications");
    this.verifyApprovalBaseUri = URI
        .create(baseUri + "/v2/Services/" + twilioConfiguration.getVerifyServiceSid() + "/Verifications/");

    this.androidAppHash = twilioConfiguration.getAndroidAppHash();

    this.httpClient = httpClient;
  }

  CompletableFuture<Optional<String>> deliverSmsVerificationWithVerify(String destination, Optional<String> clientType,
      String verificationCode, List<LanguageRange> languageRanges) {

    HttpRequest request = buildVerifyRequest("sms", destination, verificationCode, findBestLocale(languageRanges),
        clientType);

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        .thenApply(this::parseResponse)
        .handle(this::extractVerifySid);
  }

  private Optional<String> findBestLocale(List<LanguageRange> priorityList) {
    return Util.findBestLocale(priorityList, TwilioVerifySender.TWILIO_VERIFY_LANGUAGES);
  }

  private TwilioVerifyResponse parseResponse(HttpResponse<String> response) {
    ObjectMapper mapper = SystemMapper.getMapper();

    if (response.statusCode() >= 200 && response.statusCode() < 300) {
      if ("application/json".equals(response.headers().firstValue("Content-Type").orElse(null))) {
        return new TwilioVerifyResponse(TwilioVerifyResponse.SuccessResponse.fromBody(mapper, response.body()));
      } else {
        return new TwilioVerifyResponse(new TwilioVerifyResponse.SuccessResponse());
      }
    }

    if ("application/json".equals(response.headers().firstValue("Content-Type").orElse(null))) {
      return new TwilioVerifyResponse(TwilioVerifyResponse.FailureResponse.fromBody(mapper, response.body()));
    } else {
      return new TwilioVerifyResponse(new TwilioVerifyResponse.FailureResponse());
    }
  }

  CompletableFuture<Optional<String>> deliverVoxVerificationWithVerify(String destination, String verificationCode,
      List<LanguageRange> languageRanges) {

    HttpRequest request = buildVerifyRequest("call", destination, verificationCode, findBestLocale(languageRanges),
        Optional.empty());

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        .thenApply(this::parseResponse)
        .handle(this::extractVerifySid);
  }

  private Optional<String> extractVerifySid(TwilioVerifyResponse twilioVerifyResponse, Throwable throwable) {

    if (throwable != null) {
      return Optional.empty();
    }

    if (twilioVerifyResponse.isFailure()) {
      return Optional.empty();
    }

    return Optional.ofNullable(twilioVerifyResponse.successResponse.getSid());
  }

  private HttpRequest buildVerifyRequest(String channel, String destination, String verificationCode,
      Optional<String> locale, Optional<String> clientType) {

    final Map<String, String> requestParameters = new HashMap<>();
    requestParameters.put("To", destination);
    requestParameters.put("CustomCode", verificationCode);
    requestParameters.put("Channel", channel);
    locale.ifPresent(loc -> requestParameters.put("Locale", loc));
    clientType.filter(client -> client.startsWith("android"))
        .ifPresent(ignored -> requestParameters.put("AppHash", androidAppHash));

    return HttpRequest.newBuilder()
        .uri(verifyServiceUri)
        .POST(FormDataBodyPublisher.of(requestParameters))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Authorization", "Basic " + Base64.encodeBytes((accountId + ":" + accountToken).getBytes()))
        .build();
  }

  public CompletableFuture<Boolean> reportVerificationSucceeded(String verificationSid) {

    final Map<String, String> requestParameters = new HashMap<>();
    requestParameters.put("Status", "approved");

    HttpRequest request = HttpRequest.newBuilder()
        .uri(verifyApprovalBaseUri.resolve(verificationSid))
        .POST(FormDataBodyPublisher.of(requestParameters))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Authorization", "Basic " + Base64.encodeBytes((accountId + ":" + accountToken).getBytes()))
        .build();

    return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        .thenApply(this::parseResponse)
        .handle((response, throwable) -> throwable == null
            && response.isSuccess()
            && "approved".equals(response.successResponse.getStatus()));
  }

  public static class TwilioVerifyResponse {

    private SuccessResponse successResponse;
    private FailureResponse failureResponse;

    TwilioVerifyResponse(SuccessResponse successResponse) {
      this.successResponse = successResponse;
    }

    TwilioVerifyResponse(FailureResponse failureResponse) {
      this.failureResponse = failureResponse;
    }

    boolean isSuccess() {
      return successResponse != null;
    }

    boolean isFailure() {
      return failureResponse != null;
    }

    private static class SuccessResponse {

      @NotEmpty
      public String sid;

      @NotEmpty
      public String status;

      static SuccessResponse fromBody(ObjectMapper mapper, String body) {
        try {
          return mapper.readValue(body, SuccessResponse.class);
        } catch (IOException e) {
          logger.warn("Error parsing twilio success response: " + e);
          return new SuccessResponse();
        }
      }

      public String getSid() {
        return sid;
      }

      public String getStatus() {
        return status;
      }
    }

    private static class FailureResponse {

      @JsonProperty
      private int status;

      @JsonProperty
      private String message;

      @JsonProperty
      private int code;

      static FailureResponse fromBody(ObjectMapper mapper, String body) {
        try {
          return mapper.readValue(body, FailureResponse.class);
        } catch (IOException e) {
          logger.warn("Error parsing twilio response: " + e);
          return new FailureResponse();
        }
      }

    }
  }
}
