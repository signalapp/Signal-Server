package org.whispersystems.sms;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.TwilioConfiguration;
import org.whispersystems.textsecuregcm.sms.TwilioSmsSender;

import java.util.List;
import java.util.Optional;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TwilioSmsSenderTest {

  private static final String       ACCOUNT_ID            = "test_account_id";
  private static final String       ACCOUNT_TOKEN         = "test_account_token";
  private static final List<String> NUMBERS               = List.of("+14151111111", "+14152222222");
  private static final String       MESSAGING_SERVICES_ID = "test_messaging_services_id";
  private static final String       LOCAL_DOMAIN          = "test.com";

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort().dynamicHttpsPort());

  @Test
  public void testSendSms() {
    wireMockRule.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
                             .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
                             .willReturn(aResponse()
                                             .withHeader("Content-Type", "application/json")
                                             .withBody("{\"price\": -0.00750, \"status\": \"sent\"}")));


    TwilioConfiguration configuration = new TwilioConfiguration();
    configuration.setAccountId(ACCOUNT_ID);
    configuration.setAccountToken(ACCOUNT_TOKEN);
    configuration.setNumbers(NUMBERS);
    configuration.setMessagingServicesId(MESSAGING_SERVICES_ID);
    configuration.setLocalDomain(LOCAL_DOMAIN);

    TwilioSmsSender sender  = new TwilioSmsSender("http://localhost:" + wireMockRule.port(), configuration);
    boolean         success = sender.deliverSmsVerification("+14153333333", Optional.of("android-ng"), "123-456").join();

    assertThat(success).isTrue();

    verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo("MessagingServiceSid=test_messaging_services_id&To=%2B14153333333&Body=%3C%23%3E+Your+Signal+verification+code%3A+123-456%0A%0AdoDiFGKPO1r")));
  }

  @Test
  public void testSendVox() {
    wireMockRule.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Calls.json"))
                             .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
                             .willReturn(aResponse()
                                             .withHeader("Content-Type", "application/json")
                                             .withBody("{\"price\": -0.00750, \"status\": \"completed\"}")));


    TwilioConfiguration configuration = new TwilioConfiguration();
    configuration.setAccountId(ACCOUNT_ID);
    configuration.setAccountToken(ACCOUNT_TOKEN);
    configuration.setNumbers(NUMBERS);
    configuration.setMessagingServicesId(MESSAGING_SERVICES_ID);
    configuration.setLocalDomain(LOCAL_DOMAIN);

    TwilioSmsSender sender  = new TwilioSmsSender("http://localhost:" + wireMockRule.port(), configuration);
    boolean         success = sender.deliverVoxVerification("+14153333333", "123-456", Optional.of("en_US")).join();

    assertThat(success).isTrue();

    verify(1, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Calls.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(matching("To=%2B14153333333&From=%2B1415(1111111|2222222)&Url=https%3A%2F%2Ftest.com%2Fv1%2Fvoice%2Fdescription%2F123-456%3Fl%3Den_US")));
  }

  @Test
  public void testSendSmsFiveHundered() {
    wireMockRule.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
                             .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
                             .willReturn(aResponse()
                                             .withStatus(500)
                                             .withHeader("Content-Type", "application/json")
                                             .withBody("{\"message\": \"Server error!\"}")));


    TwilioConfiguration configuration = new TwilioConfiguration();
    configuration.setAccountId(ACCOUNT_ID);
    configuration.setAccountToken(ACCOUNT_TOKEN);
    configuration.setNumbers(NUMBERS);
    configuration.setMessagingServicesId(MESSAGING_SERVICES_ID);
    configuration.setLocalDomain(LOCAL_DOMAIN);

    TwilioSmsSender sender  = new TwilioSmsSender("http://localhost:" + wireMockRule.port(), configuration);
    boolean         success = sender.deliverSmsVerification("+14153333333", Optional.of("android-ng"), "123-456").join();

    assertThat(success).isFalse();

    verify(3, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Messages.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo("MessagingServiceSid=test_messaging_services_id&To=%2B14153333333&Body=%3C%23%3E+Your+Signal+verification+code%3A+123-456%0A%0AdoDiFGKPO1r")));
  }

  @Test
  public void testSendVoxFiveHundred() {
    wireMockRule.stubFor(post(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Calls.json"))
                             .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
                             .willReturn(aResponse()
                                             .withStatus(500)
                                             .withHeader("Content-Type", "application/json")
                                             .withBody("{\"message\": \"Server error!\"}")));

    TwilioConfiguration configuration = new TwilioConfiguration();
    configuration.setAccountId(ACCOUNT_ID);
    configuration.setAccountToken(ACCOUNT_TOKEN);
    configuration.setNumbers(NUMBERS);
    configuration.setMessagingServicesId(MESSAGING_SERVICES_ID);
    configuration.setLocalDomain(LOCAL_DOMAIN);

    TwilioSmsSender sender  = new TwilioSmsSender("http://localhost:" + wireMockRule.port(), configuration);
    boolean         success = sender.deliverVoxVerification("+14153333333", "123-456", Optional.of("en_US")).join();

    assertThat(success).isFalse();

    verify(3, postRequestedFor(urlEqualTo("/2010-04-01/Accounts/" + ACCOUNT_ID + "/Calls.json"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(matching("To=%2B14153333333&From=%2B1415(1111111|2222222)&Url=https%3A%2F%2Ftest.com%2Fv1%2Fvoice%2Fdescription%2F123-456%3Fl%3Den_US")));

  }

  @Test
  public void testSendSmsNetworkFailure() {
    TwilioConfiguration configuration = new TwilioConfiguration();
    configuration.setAccountId(ACCOUNT_ID);
    configuration.setAccountToken(ACCOUNT_TOKEN);
    configuration.setNumbers(NUMBERS);
    configuration.setMessagingServicesId(MESSAGING_SERVICES_ID);
    configuration.setLocalDomain(LOCAL_DOMAIN);

    TwilioSmsSender sender  = new TwilioSmsSender("http://localhost:" + 39873, configuration);
    boolean         success = sender.deliverSmsVerification("+14153333333", Optional.of("android-ng"), "123-456").join();

    assertThat(success).isFalse();
  }



}
