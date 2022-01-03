/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.sms;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Locale.LanguageRange;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.configuration.TwilioConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.util.ExecutorUtils;

@SuppressWarnings("OptionalGetWithoutIsPresent")
class TwilioVerifySenderTest {

  private static final String ACCOUNT_ID = "test_account_id";
  private static final String ACCOUNT_TOKEN = "test_account_token";
  private static final String MESSAGING_SERVICE_SID = "test_messaging_services_id";
  private static final String NANPA_MESSAGING_SERVICE_SID = "nanpa_test_messaging_service_id";
  private static final String VERIFY_SERVICE_SID = "verify_service_sid";
  private static final String LOCAL_DOMAIN = "test.com";
  private static final String ANDROID_APP_HASH = "someHash";
  private static final String SERVICE_FRIENDLY_NAME = "SignalTest";

  private static final String VERIFICATION_SID = "verification";

  @RegisterExtension
  private final WireMockExtension wireMock = WireMockExtension.newInstance()
      .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
      .build();

  private TwilioVerifySender sender;

  @BeforeEach
  void setup() {
    final TwilioConfiguration twilioConfiguration = createTwilioConfiguration();

    final FaultTolerantHttpClient httpClient = FaultTolerantHttpClient.newBuilder()
        .withCircuitBreaker(twilioConfiguration.getCircuitBreaker())
        .withRetry(twilioConfiguration.getRetry())
        .withVersion(HttpClient.Version.HTTP_2)
        .withConnectTimeout(Duration.ofSeconds(10))
        .withRedirect(HttpClient.Redirect.NEVER)
        .withExecutor(ExecutorUtils.newFixedThreadBoundedQueueExecutor(10, 100))
        .withName("twilio")
        .build();

    sender = new TwilioVerifySender("http://localhost:" + wireMock.getPort(), httpClient, twilioConfiguration);
  }

  private TwilioConfiguration createTwilioConfiguration() {

    TwilioConfiguration configuration = new TwilioConfiguration();

    configuration.setAccountId(ACCOUNT_ID);
    configuration.setAccountToken(ACCOUNT_TOKEN);
    configuration.setMessagingServiceSid(MESSAGING_SERVICE_SID);
    configuration.setNanpaMessagingServiceSid(NANPA_MESSAGING_SERVICE_SID);
    configuration.setVerifyServiceSid(VERIFY_SERVICE_SID);
    configuration.setLocalDomain(LOCAL_DOMAIN);
    configuration.setAndroidAppHash(ANDROID_APP_HASH);
    configuration.setVerifyServiceFriendlyName(SERVICE_FRIENDLY_NAME);

    return configuration;
  }

  private void setupSuccessStubForVerify() {
    wireMock.stubFor(post(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications"))
        .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
        .willReturn(aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody("{\"sid\": \"" + VERIFICATION_SID + "\", \"status\": \"pending\"}")));
  }

  @ParameterizedTest
  @MethodSource
  void deliverSmsVerificationWithVerify(@Nullable final String client, @Nullable final String languageRange,
      final boolean expectAppHash, @Nullable final String expectedLocale) throws Exception {

    setupSuccessStubForVerify();

    List<LanguageRange> languageRanges = Optional.ofNullable(languageRange)
        .map(LanguageRange::parse)
        .orElse(Collections.emptyList());

    final Optional<String> verificationSid = sender
        .deliverSmsVerificationWithVerify("+14153333333", Optional.ofNullable(client), "123456",
            languageRanges).get();

    assertEquals(VERIFICATION_SID, verificationSid.get());

    wireMock.verify(1, postRequestedFor(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo(
            (expectedLocale == null ? "" : "Locale=" + expectedLocale + "&")
                + "Channel=sms&To=%2B14153333333&CustomFriendlyName=" + SERVICE_FRIENDLY_NAME
                + "&CustomCode=123456" + (expectAppHash ? "&AppHash=" + ANDROID_APP_HASH : "")
        )));
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> deliverSmsVerificationWithVerify() {
    return Stream.of(
        // client, languageRange, expectAppHash, expectedLocale
        Arguments.of("ios", "fr-CA, en", false, "fr"),
        Arguments.of("android-2021-03", "zh-HK, it", true, "zh-HK"),
        Arguments.of(null, null, false, null)
    );
  }

  @ParameterizedTest
  @MethodSource
  void deliverVoxVerificationWithVerify(@Nullable final String languageRange,
      @Nullable final String expectedLocale) throws Exception {

    setupSuccessStubForVerify();

    final List<LanguageRange> languageRanges = Optional.ofNullable(languageRange)
        .map(LanguageRange::parse)
        .orElse(Collections.emptyList());

    final Optional<String> verificationSid = sender
        .deliverVoxVerificationWithVerify("+14153333333", "123456", languageRanges).get();

    assertEquals(VERIFICATION_SID, verificationSid.get());

    wireMock.verify(1, postRequestedFor(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo(
            (expectedLocale == null ? "" : "Locale=" + expectedLocale + "&")
                + "Channel=call&To=%2B14153333333&CustomFriendlyName=" + SERVICE_FRIENDLY_NAME
                + "&CustomCode=123456")));
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> deliverVoxVerificationWithVerify() {
    return Stream.of(
        // languageRange, expectedLocale
        Arguments.of("fr-CA, en", "fr"),
        Arguments.of("zh-HK, it", "zh-HK"),
        Arguments.of("en-CAA, en", "en"),
        Arguments.of(null, null)
    );
  }

  @Test
  void testSmsFiveHundred() throws Exception {
    wireMock.stubFor(post(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications"))
        .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
        .willReturn(aResponse()
            .withStatus(500)
            .withHeader("Content-Type", "application/json")
            .withBody("{\"message\": \"Server error!\"}")));

    final Optional<String> verificationSid = sender
        .deliverSmsVerificationWithVerify("+14153333333", Optional.empty(), "123456", Collections.emptyList()).get();

    assertThat(verificationSid).isEmpty();

    wireMock.verify(3, postRequestedFor(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo("Channel=sms&To=%2B14153333333&CustomFriendlyName=" + SERVICE_FRIENDLY_NAME
            + "&CustomCode=123456")));
  }

  @Test
  void testVoxFiveHundred() throws Exception {
    wireMock.stubFor(post(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications"))
        .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
        .willReturn(aResponse()
            .withStatus(500)
            .withHeader("Content-Type", "application/json")
            .withBody("{\"message\": \"Server error!\"}")));

    final Optional<String> verificationSid = sender
        .deliverVoxVerificationWithVerify("+14153333333", "123456", Collections.emptyList()).get();

    assertThat(verificationSid).isEmpty();

    wireMock.verify(3, postRequestedFor(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo("Channel=call&To=%2B14153333333&CustomFriendlyName=" + SERVICE_FRIENDLY_NAME
            + "&CustomCode=123456")));
  }

  @Test
  void reportVerificationSucceeded() throws Exception {

    wireMock.stubFor(post(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications/" + VERIFICATION_SID))
        .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("{\"status\": \"approved\", \"sid\": \"" + VERIFICATION_SID + "\"}")));

    final Boolean success = sender.reportVerificationSucceeded(VERIFICATION_SID).get();

    assertThat(success).isTrue();

    wireMock.verify(1,
        postRequestedFor(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications/" + VERIFICATION_SID))
            .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
            .withRequestBody(equalTo("Status=approved")));
  }
}
