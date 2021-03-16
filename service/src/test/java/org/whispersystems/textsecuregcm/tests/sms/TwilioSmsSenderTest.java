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
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.*;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.TwilioConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicTwilioConfiguration;
import org.whispersystems.textsecuregcm.sms.TwilioSmsSender;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class TwilioSmsSenderTest {

  private static final String       ACCOUNT_ID                  = "test_account_id";
  private static final String       ACCOUNT_TOKEN               = "test_account_token";
  private static final String       MESSAGING_SERVICE_SID       = "test_messaging_services_id";
  private static final String       NANPA_MESSAGING_SERVICE_SID = "nanpa_test_messaging_service_id";
  private static final String       LOCAL_DOMAIN                = "test.com";

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort().dynamicHttpsPort());

  private DynamicConfigurationManager dynamicConfigurationManager;

  @Before
  public void setup() {

    dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    DynamicConfiguration dynamicConfiguration = new DynamicConfiguration();
    DynamicTwilioConfiguration dynamicTwilioConfiguration = new DynamicTwilioConfiguration();
    dynamicConfiguration.setTwilioConfiguration(dynamicTwilioConfiguration);
    dynamicTwilioConfiguration.setNumbers(List.of("+14151111111", "+14152222222"));
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
  }

  @Nonnull
  private TwilioConfiguration createTwilioConfiguration() {
    TwilioConfiguration configuration = new TwilioConfiguration();
    configuration.setAccountId(ACCOUNT_ID);
    configuration.setAccountToken(ACCOUNT_TOKEN);
    configuration.setMessagingServiceSid(MESSAGING_SERVICE_SID);
    configuration.setNanpaMessagingServiceSid(NANPA_MESSAGING_SERVICE_SID);
    configuration.setLocalDomain(LOCAL_DOMAIN);
    configuration.setIosVerificationText("Verify on iOS: %1$s\n\nsomelink://verify/%1$s");
    configuration.setAndroidNgVerificationText("<#> Verify on AndroidNg: %1$s\n\ncharacters");
    configuration.setAndroid202001VerificationText("Verify on Android202001: %1$s\n\nsomelink://verify/%1$s\n\ncharacters");
    configuration.setGenericVerificationText("Verify on whatever: %1$s");
    return configuration;
  }

  private void setupSuccessStubForSms() {
    wireMockRule.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
                                 .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
                                 .willReturn(aResponse()
                                                     .withHeader("Content-Type", "application/json")
                                                     .withBody("{\"price\": -0.00750, \"status\": \"sent\"}")));
  }

  @Test
  public void testSendSms() {
    setupSuccessStubForSms();
    TwilioConfiguration configuration = createTwilioConfiguration();
    TwilioSmsSender sender  = new TwilioSmsSender("http://localhost:" + wireMockRule.port(), configuration, dynamicConfigurationManager);
    boolean         success = sender.deliverSmsVerification("+14153333333", Optional.of("android-ng"), "123-456").join();

    assertThat(success).isTrue();

    verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo("MessagingServiceSid=nanpa_test_messaging_service_id&To=%2B14153333333&Body=%3C%23%3E+Verify+on+AndroidNg%3A+123-456%0A%0Acharacters")));
  }

  @Test
  public void testSendSmsAndroid202001() {
    setupSuccessStubForSms();
    TwilioConfiguration configuration = createTwilioConfiguration();
    TwilioSmsSender sender = new TwilioSmsSender("http://localhost:" + wireMockRule.port(), configuration, dynamicConfigurationManager);
    boolean success = sender.deliverSmsVerification("+14153333333", Optional.of("android-2020-01"), "123-456").join();

    assertThat(success).isTrue();

    verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
            .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
            .withRequestBody(equalTo("MessagingServiceSid=nanpa_test_messaging_service_id&To=%2B14153333333&Body=Verify+on+Android202001%3A+123-456%0A%0Asomelink%3A%2F%2Fverify%2F123-456%0A%0Acharacters")));
  }

  @Test
  public void testSendSmsNanpaMessagingService() {
    setupSuccessStubForSms();
    TwilioConfiguration configuration = createTwilioConfiguration();
    configuration.setNanpaMessagingServiceSid(NANPA_MESSAGING_SERVICE_SID);
    TwilioSmsSender sender = new TwilioSmsSender("http://localhost:" + wireMockRule.port(), configuration, dynamicConfigurationManager);

    assertThat(sender.deliverSmsVerification("+14153333333", Optional.of("ios"), "654-321").join()).isTrue();
    verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
            .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
            .withRequestBody(equalTo("MessagingServiceSid=nanpa_test_messaging_service_id&To=%2B14153333333&Body=Verify+on+iOS%3A+654-321%0A%0Asomelink%3A%2F%2Fverify%2F654-321")));

    wireMockRule.resetRequests();
    assertThat(sender.deliverSmsVerification("+447911123456", Optional.of("ios"), "654-321").join()).isTrue();
    verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
           .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
           .withRequestBody(equalTo("MessagingServiceSid=test_messaging_services_id&To=%2B447911123456&Body=Verify+on+iOS%3A+654-321%0A%0Asomelink%3A%2F%2Fverify%2F654-321")));
  }

  @Test
  public void testSendVox() {
    wireMockRule.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Calls.json"))
                             .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
                             .willReturn(aResponse()
                                             .withHeader("Content-Type", "application/json")
                                             .withBody("{\"price\": -0.00750, \"status\": \"completed\"}")));


    TwilioConfiguration configuration = createTwilioConfiguration();

    TwilioSmsSender sender  = new TwilioSmsSender("http://localhost:" + wireMockRule.port(), configuration, dynamicConfigurationManager);
    boolean         success = sender.deliverVoxVerification("+14153333333", "123-456", Optional.of("en_US")).join();

    assertThat(success).isTrue();

    verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Calls.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(matching("To=%2B14153333333&From=%2B1415(1111111|2222222)&Url=https%3A%2F%2Ftest.com%2Fv1%2Fvoice%2Fdescription%2F123-456%3Fl%3Den_US")));
  }

  @Test
  public void testSendSmsFiveHundred() {
    wireMockRule.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
                             .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
                             .willReturn(aResponse()
                                             .withStatus(500)
                                             .withHeader("Content-Type", "application/json")
                                             .withBody("{\"message\": \"Server error!\"}")));


    TwilioConfiguration configuration = createTwilioConfiguration();

    TwilioSmsSender sender  = new TwilioSmsSender("http://localhost:" + wireMockRule.port(), configuration, dynamicConfigurationManager);
    boolean         success = sender.deliverSmsVerification("+14153333333", Optional.of("android-ng"), "123-456").join();

    assertThat(success).isFalse();

    verify(3, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo("MessagingServiceSid=nanpa_test_messaging_service_id&To=%2B14153333333&Body=%3C%23%3E+Verify+on+AndroidNg%3A+123-456%0A%0Acharacters")));
  }

  @Test
  public void testSendVoxFiveHundred() {
    wireMockRule.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Calls.json"))
                             .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
                             .willReturn(aResponse()
                                             .withStatus(500)
                                             .withHeader("Content-Type", "application/json")
                                             .withBody("{\"message\": \"Server error!\"}")));

    TwilioConfiguration configuration = createTwilioConfiguration();

    TwilioSmsSender sender  = new TwilioSmsSender("http://localhost:" + wireMockRule.port(), configuration, dynamicConfigurationManager);
    boolean         success = sender.deliverVoxVerification("+14153333333", "123-456", Optional.of("en_US")).join();

    assertThat(success).isFalse();

    verify(3, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Calls.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(matching("To=%2B14153333333&From=%2B1415(1111111|2222222)&Url=https%3A%2F%2Ftest.com%2Fv1%2Fvoice%2Fdescription%2F123-456%3Fl%3Den_US")));

  }

  @Test
  public void testSendSmsNetworkFailure() {
    TwilioConfiguration configuration = createTwilioConfiguration();

    TwilioSmsSender sender  = new TwilioSmsSender("http://localhost:" + 39873, configuration, dynamicConfigurationManager);
    boolean         success = sender.deliverSmsVerification("+14153333333", Optional.of("android-ng"), "123-456").join();

    assertThat(success).isFalse();
  }

  @Test
  public void testRetrySmsOnUnreachableErrorCodeIsTriedOnlyOnceWithoutSenderId() {
    wireMockRule.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
                                 .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
                                 .willReturn(aResponse()
                                                     .withStatus(400)
                                                     .withHeader("Content-Type", "application/json")
                                                     .withBody("{\"status\": 400, \"message\": \"is not currently reachable\", \"code\": 21612}")));

    TwilioConfiguration configuration = createTwilioConfiguration();

    TwilioSmsSender sender  = new TwilioSmsSender("http://localhost:" + wireMockRule.port(), configuration, dynamicConfigurationManager);
    boolean         success = sender.deliverSmsVerification("+14153333333", Optional.of("android-ng"), "123-456").join();

    assertThat(success).isFalse();

    verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
            .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
            .withRequestBody(equalTo("MessagingServiceSid=nanpa_test_messaging_service_id&To=%2B14153333333&Body=%3C%23%3E+Verify+on+AndroidNg%3A+123-456%0A%0Acharacters")));
  }

  @Test
  public void testSendSmsChina() {
    setupSuccessStubForSms();
    TwilioConfiguration configuration = createTwilioConfiguration();
    TwilioSmsSender sender  = new TwilioSmsSender("http://localhost:" + wireMockRule.port(), configuration, dynamicConfigurationManager);
    boolean success = sender.deliverSmsVerification("+861065529988", Optional.of("android-ng"), "123-456").join();

    assertThat(success).isTrue();

    verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
            .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
            .withRequestBody(equalTo("MessagingServiceSid=test_messaging_services_id&To=%2B861065529988&Body=%3C%23%3E+Verify+on+AndroidNg%3A+123-456%0A%0Acharacters%E2%80%88")));
  }
}
