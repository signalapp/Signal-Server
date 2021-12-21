/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.sms;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import java.util.List;
import java.util.Locale.LanguageRange;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.TwilioConfiguration;
import org.whispersystems.textsecuregcm.configuration.TwilioVerificationTextConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicTwilioConfiguration;
import org.whispersystems.textsecuregcm.sms.TwilioSmsSender;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

class TwilioSmsSenderTest {

  private static final String       ACCOUNT_ID                  = "test_account_id";
  private static final String       ACCOUNT_TOKEN               = "test_account_token";
  private static final String       MESSAGING_SERVICE_SID       = "test_messaging_services_id";
  private static final String       NANPA_MESSAGING_SERVICE_SID = "nanpa_test_messaging_service_id";
  private static final String       VERIFY_SERVICE_SID          = "verify_service_sid";
  private static final String       LOCAL_DOMAIN                = "test.com";

  @RegisterExtension
  private final WireMockExtension wireMock = WireMockExtension.newInstance()
      .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
      .build();

  private DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private TwilioSmsSender sender;

  @BeforeEach
  void setup() {

    dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    DynamicConfiguration dynamicConfiguration = new DynamicConfiguration();
    DynamicTwilioConfiguration dynamicTwilioConfiguration = new DynamicTwilioConfiguration();
    dynamicConfiguration.setTwilioConfiguration(dynamicTwilioConfiguration);
    dynamicTwilioConfiguration.setNumbers(List.of("+14151111111", "+14152222222"));
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    TwilioConfiguration configuration = createTwilioConfiguration();
    sender = new TwilioSmsSender("http://localhost:" + wireMock.getPort(), "http://localhost:11111", configuration, dynamicConfigurationManager);
  }

  @Nonnull
  private TwilioConfiguration createTwilioConfiguration() {
    TwilioConfiguration configuration = new TwilioConfiguration();
    configuration.setAccountId(ACCOUNT_ID);
    configuration.setAccountToken(ACCOUNT_TOKEN);
    configuration.setMessagingServiceSid(MESSAGING_SERVICE_SID);
    configuration.setNanpaMessagingServiceSid(NANPA_MESSAGING_SERVICE_SID);
    configuration.setVerifyServiceSid(VERIFY_SERVICE_SID);
    configuration.setLocalDomain(LOCAL_DOMAIN);

    configuration.setDefaultClientVerificationTexts(createTwlilioVerificationText(""));

    configuration.setRegionalClientVerificationTexts(
        Map.of("33", createTwlilioVerificationText("[33] "))
    );
    configuration.setAndroidAppHash("someHash");
    return configuration;
  }

  private TwilioVerificationTextConfiguration createTwlilioVerificationText(final String prefix) {

    TwilioVerificationTextConfiguration verificationTextConfiguration = new TwilioVerificationTextConfiguration();

    verificationTextConfiguration.setIosText(prefix + "Verify on iOS: %1$s\n\nsomelink://verify/%1$s");
    verificationTextConfiguration.setAndroidNgText(prefix + "<#> Verify on AndroidNg: %1$s\n\ncharacters");
    verificationTextConfiguration.setAndroid202001Text(prefix + "Verify on Android202001: %1$s\n\nsomelink://verify/%1$s\n\ncharacters");
    verificationTextConfiguration.setAndroid202103Text(prefix + "Verify on Android202103: %1$s\n\ncharacters");
    verificationTextConfiguration.setGenericText(prefix + "Verify on whatever: %1$s");

    return verificationTextConfiguration;
  }

  private void setupSuccessStubForSms() {
    wireMock.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
                                 .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
                                 .willReturn(aResponse()
                                                     .withHeader("Content-Type", "application/json")
                                                     .withBody("{\"price\": -0.00750, \"status\": \"sent\"}")));
  }

  @Test
  void testSendSms() {
    setupSuccessStubForSms();

    boolean success = sender.deliverSmsVerification("+14153333333", Optional.of("android-ng"), "123-456").join();

    assertThat(success).isTrue();

    wireMock.verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo("MessagingServiceSid=nanpa_test_messaging_service_id&To=%2B14153333333&Body=%3C%23%3E+Verify+on+AndroidNg%3A+123-456%0A%0Acharacters")));
  }

  @Test
  void testSendSmsAndroid202001() {
    setupSuccessStubForSms();

    boolean success = sender.deliverSmsVerification("+14153333333", Optional.of("android-2020-01"), "123-456").join();

    assertThat(success).isTrue();

    wireMock.verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
            .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
            .withRequestBody(equalTo("MessagingServiceSid=nanpa_test_messaging_service_id&To=%2B14153333333&Body=Verify+on+Android202001%3A+123-456%0A%0Asomelink%3A%2F%2Fverify%2F123-456%0A%0Acharacters")));
  }

  @Test
  void testSendSmsAndroid202103() {
    setupSuccessStubForSms();

    boolean success = sender.deliverSmsVerification("+14153333333", Optional.of("android-2021-03"), "123456").join();

    assertThat(success).isTrue();

    wireMock.verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo("MessagingServiceSid=nanpa_test_messaging_service_id&To=%2B14153333333&Body=Verify+on+Android202103%3A+123456%0A%0Acharacters")));
  }

  @Test
  void testSendSmsNanpaMessagingService() {
    setupSuccessStubForSms();
    TwilioConfiguration configuration = createTwilioConfiguration();
    configuration.setNanpaMessagingServiceSid(NANPA_MESSAGING_SERVICE_SID);

    TwilioSmsSender sender = new TwilioSmsSender("http://localhost:" + wireMock.getPort(),
        "http://localhost:11111", configuration, dynamicConfigurationManager);

    assertThat(sender.deliverSmsVerification("+14153333333", Optional.of("ios"), "654-321").join()).isTrue();
    wireMock.verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
            .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
            .withRequestBody(equalTo("MessagingServiceSid=nanpa_test_messaging_service_id&To=%2B14153333333&Body=Verify+on+iOS%3A+654-321%0A%0Asomelink%3A%2F%2Fverify%2F654-321")));

    wireMock.resetRequests();
    assertThat(sender.deliverSmsVerification("+447911123456", Optional.of("ios"), "654-321").join()).isTrue();
    wireMock.verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
           .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
           .withRequestBody(equalTo("MessagingServiceSid=test_messaging_services_id&To=%2B447911123456&Body=Verify+on+iOS%3A+654-321%0A%0Asomelink%3A%2F%2Fverify%2F654-321")));
  }

  @Test
  void testSendVox() {
    wireMock.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Calls.json"))
                             .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
                             .willReturn(aResponse()
                                             .withHeader("Content-Type", "application/json")
                                             .withBody("{\"price\": -0.00750, \"status\": \"completed\"}")));

    boolean success = sender.deliverVoxVerification("+14153333333", "123-456", LanguageRange.parse("en-US")).join();

    assertThat(success).isTrue();

    wireMock.verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Calls.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(matching("To=%2B14153333333&From=%2B1415(1111111|2222222)&Url=https%3A%2F%2Ftest.com%2Fv1%2Fvoice%2Fdescription%2F123-456%3Fl%3Den-US")));
  }

  @Test
  void testSendVoxMultipleLocales() {
    wireMock.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Calls.json"))
        .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
        .willReturn(aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody("{\"price\": -0.00750, \"status\": \"completed\"}")));

    boolean success = sender.deliverVoxVerification("+14153333333", "123-456", LanguageRange.parse("en-US;q=1, ar-US;q=0.9, fa-US;q=0.8, zh-Hans-US;q=0.7, ru-RU;q=0.6, zh-Hant-US;q=0.5")).join();

    assertThat(success).isTrue();

    wireMock.verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Calls.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(matching("To=%2B14153333333&From=%2B1415(1111111|2222222)&Url=https%3A%2F%2Ftest.com%2Fv1%2Fvoice%2Fdescription%2F123-456%3Fl%3Den-US%26l%3Dar-US%26l%3Dfa-US%26l%3Dzh-US%26l%3Dru-RU%26l%3Dzh-US")));
  }

  @Test
  void testSendSmsFiveHundred() {
    wireMock.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
                             .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
                             .willReturn(aResponse()
                                             .withStatus(500)
                                             .withHeader("Content-Type", "application/json")
                                             .withBody("{\"message\": \"Server error!\"}")));

    boolean success = sender.deliverSmsVerification("+14153333333", Optional.of("android-ng"), "123-456").join();

    assertThat(success).isFalse();

    wireMock.verify(3, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo("MessagingServiceSid=nanpa_test_messaging_service_id&To=%2B14153333333&Body=%3C%23%3E+Verify+on+AndroidNg%3A+123-456%0A%0Acharacters")));
  }

  @Test
  void testSendVoxFiveHundred() {
    wireMock.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Calls.json"))
                             .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
                             .willReturn(aResponse()
                                             .withStatus(500)
                                             .withHeader("Content-Type", "application/json")
                                             .withBody("{\"message\": \"Server error!\"}")));

    boolean success = sender.deliverVoxVerification("+14153333333", "123-456", LanguageRange.parse("en-US")).join();

    assertThat(success).isFalse();

    wireMock.verify(3, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Calls.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(matching("To=%2B14153333333&From=%2B1415(1111111|2222222)&Url=https%3A%2F%2Ftest.com%2Fv1%2Fvoice%2Fdescription%2F123-456%3Fl%3Den-US")));

  }

  @Test
  void testSendSmsNetworkFailure() {
    TwilioConfiguration configuration = createTwilioConfiguration();
    TwilioSmsSender sender = new TwilioSmsSender("http://localhost:" + 39873, "http://localhost:" + 39873, configuration, dynamicConfigurationManager);

    boolean success = sender.deliverSmsVerification("+14153333333", Optional.of("android-ng"), "123-456").join();

    assertThat(success).isFalse();
  }

  @Test
  void testRetrySmsOnUnreachableErrorCodeIsTriedOnlyOnceWithoutSenderId() {
    wireMock.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
                                 .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
                                 .willReturn(aResponse()
                                                     .withStatus(400)
                                                     .withHeader("Content-Type", "application/json")
                                                     .withBody("{\"status\": 400, \"message\": \"is not currently reachable\", \"code\": 21612}")));

    boolean success = sender.deliverSmsVerification("+14153333333", Optional.of("android-ng"), "123-456").join();

    assertThat(success).isFalse();

    wireMock.verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
            .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
            .withRequestBody(equalTo("MessagingServiceSid=nanpa_test_messaging_service_id&To=%2B14153333333&Body=%3C%23%3E+Verify+on+AndroidNg%3A+123-456%0A%0Acharacters")));
  }

  @Test
  void testSendSmsChina() {
    setupSuccessStubForSms();

    boolean success = sender.deliverSmsVerification("+861065529988", Optional.of("android-ng"), "123-456").join();

    assertThat(success).isTrue();

    wireMock.verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
            .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
            .withRequestBody(equalTo("MessagingServiceSid=test_messaging_services_id&To=%2B861065529988&Body=%3C%23%3E+Verify+on+AndroidNg%3A+123-456%0A%0Acharacters%E2%80%88")));
  }

  @Test
  void testSendSmsRegionalVerificationText() {
    setupSuccessStubForSms();

    boolean success = sender.deliverSmsVerification("+33655512673", Optional.of("android-ng"), "123-456").join();

    assertThat(success).isTrue();

    wireMock.verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo("MessagingServiceSid=test_messaging_services_id&To=%2B33655512673&Body=%5B33%5D+%3C%23%3E+Verify+on+AndroidNg%3A+123-456%0A%0Acharacters")));
  }

}
