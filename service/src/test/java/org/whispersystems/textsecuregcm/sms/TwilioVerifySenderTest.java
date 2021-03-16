package org.whispersystems.textsecuregcm.sms;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Locale.LanguageRange;
import java.util.Optional;
import javax.annotation.Nullable;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.whispersystems.textsecuregcm.configuration.TwilioConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.util.ExecutorUtils;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(JUnitParamsRunner.class)
public class TwilioVerifySenderTest {

  private static final String ACCOUNT_ID = "test_account_id";
  private static final String ACCOUNT_TOKEN = "test_account_token";
  private static final String MESSAGING_SERVICE_SID = "test_messaging_services_id";
  private static final String NANPA_MESSAGING_SERVICE_SID = "nanpa_test_messaging_service_id";
  private static final String VERIFY_SERVICE_SID = "verify_service_sid";
  private static final String LOCAL_DOMAIN = "test.com";
  private static final String ANDROID_APP_HASH = "someHash";

  private static final String VERIFICATION_SID = "verification";

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort().dynamicHttpsPort());

  private TwilioVerifySender sender;

  @Before
  public void setup() {
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

    sender = new TwilioVerifySender("http://localhost:" + wireMockRule.port(), httpClient, twilioConfiguration);
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

    return configuration;
  }

  private void setupSuccessStubForVerify() {
    wireMockRule.stubFor(post(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications"))
        .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
        .willReturn(aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody("{\"sid\": \"" + VERIFICATION_SID + "\", \"status\": \"pending\"}")));
  }

  @Test
  @Parameters(method = "argumentsForDeliverSmsVerificationWithVerify")
  public void deliverSmsVerificationWithVerify(@Nullable final String client, @Nullable final String languageRange,
      final boolean expectAppHash, @Nullable final String expectedLocale) throws Exception {

    setupSuccessStubForVerify();

    List<LanguageRange> languageRanges = Optional.ofNullable(languageRange)
        .map(LanguageRange::parse)
        .orElse(Collections.emptyList());

    final Optional<String> verificationSid = sender
        .deliverSmsVerificationWithVerify("+14153333333", Optional.ofNullable(client), "123456",
            languageRanges).get();

    assertEquals(VERIFICATION_SID, verificationSid.get());

    verify(1, postRequestedFor(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo(
            (expectedLocale == null ? "" : "Locale=" + expectedLocale + "&")
                + "Channel=sms&To=%2B14153333333&CustomCode=123456"
                + (expectAppHash ? "&AppHash=" + ANDROID_APP_HASH : "")
        )));
  }

  private static Object argumentsForDeliverSmsVerificationWithVerify() {
    return new Object[][]{
        // client, languageRange, expectAppHash, expectedLocale
        {"ios", "fr-CA, en", false, "fr"},
        {"android-2021-03", "zh-HK, it", true, "zh-HK"},
        {null, null, false, null}
    };
  }

  @Test
  @Parameters(method = "argumentsForDeliverVoxVerificationWithVerify")
  public void deliverVoxVerificationWithVerify(@Nullable final String languageRange,
      @Nullable final String expectedLocale) throws Exception {

    setupSuccessStubForVerify();

    final List<LanguageRange> languageRanges = Optional.ofNullable(languageRange)
        .map(LanguageRange::parse)
        .orElse(Collections.emptyList());

    final Optional<String> verificationSid = sender
        .deliverVoxVerificationWithVerify("+14153333333", "123456", languageRanges).get();

    assertEquals(VERIFICATION_SID, verificationSid.get());

    verify(1, postRequestedFor(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo(
            (expectedLocale == null ? "" : "Locale=" + expectedLocale + "&")
                + "Channel=call&To=%2B14153333333&CustomCode=123456")));
  }

  private static Object argumentsForDeliverVoxVerificationWithVerify() {
    return new Object[][]{
        // languageRange, expectedLocale
        {"fr-CA, en", "fr"},
        {"zh-HK, it", "zh-HK"},
        {"en-CAA, en", "en"},
        {null, null}
    };
  }

  @Test
  public void testSmsFiveHundred() throws Exception {
    wireMockRule.stubFor(post(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications"))
        .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
        .willReturn(aResponse()
            .withStatus(500)
            .withHeader("Content-Type", "application/json")
            .withBody("{\"message\": \"Server error!\"}")));

    final Optional<String> verificationSid = sender
        .deliverSmsVerificationWithVerify("+14153333333", Optional.empty(), "123456", Collections.emptyList()).get();

    assertThat(verificationSid).isEmpty();

    verify(3, postRequestedFor(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo("Channel=sms&To=%2B14153333333&CustomCode=123456")));
  }

  @Test
  public void testVoxFiveHundred() throws Exception {
    wireMockRule.stubFor(post(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications"))
        .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
        .willReturn(aResponse()
            .withStatus(500)
            .withHeader("Content-Type", "application/json")
            .withBody("{\"message\": \"Server error!\"}")));

    final Optional<String> verificationSid = sender
        .deliverVoxVerificationWithVerify("+14153333333", "123456", Collections.emptyList()).get();

    assertThat(verificationSid).isEmpty();

    verify(3, postRequestedFor(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications"))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo("Channel=call&To=%2B14153333333&CustomCode=123456")));
  }

  @Test
  public void reportVerificationSucceeded() throws Exception {

    wireMockRule.stubFor(post(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications/" + VERIFICATION_SID))
        .withBasicAuth(ACCOUNT_ID, ACCOUNT_TOKEN)
        .willReturn(aResponse()
        .withStatus(200)
        .withHeader("Content-Type", "application/json")
        .withBody("{\"status\": \"approved\", \"sid\": \"" + VERIFICATION_SID + "\"}")));

    final Boolean success = sender.reportVerificationSucceeded(VERIFICATION_SID).get();

    assertThat(success).isTrue();

    verify(1, postRequestedFor(urlEqualTo("/v2/Services/" + VERIFY_SERVICE_SID + "/Verifications/" + VERIFICATION_SID))
        .withHeader("Content-Type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo("Status=approved")));
  }
}
