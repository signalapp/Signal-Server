package org.whispersystems.textsecuregcm.captcha;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicCaptchaConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class HCaptchaClientTest {

  private static final String SITE_KEY = "site-key";
  private static final String TOKEN = "token";
  private static final String USER_AGENT = "user-agent";


  static Stream<Arguments> captchaProcessed() {
    return Stream.of(
        // hCaptcha scores are inverted compared to recaptcha scores. (low score is good)
        Arguments.of(true, 0.4f, 0.5f, true),
        Arguments.of(false, 0.4f, 0.5f, false),
        Arguments.of(true, 0.6f, 0.5f, false),
        Arguments.of(false, 0.6f, 0.5f, false),
        Arguments.of(true, 0.6f, 0.4f, true),
        Arguments.of(true, 0.61f, 0.4f, false),
        Arguments.of(true, 0.7f, 0.3f, true)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void captchaProcessed(final boolean success, final float hCaptchaScore, final float scoreFloor, final boolean expectedResult)
      throws IOException, InterruptedException {

    final FaultTolerantHttpClient client = mockResponder(200, String.format("""
            {
              "success": %b,
              "score": %f,
              "score-reasons": ["great job doing this captcha"]
            }
            """,
        success, hCaptchaScore));

    final AssessmentResult result = new HCaptchaClient("fake", client, mockConfig(true, scoreFloor))
        .verify(SITE_KEY, Action.CHALLENGE, TOKEN, null, USER_AGENT);
    if (!success) {
      assertThat(result).isEqualTo(AssessmentResult.invalid());
    } else {
      assertThat(result.isValid()).isEqualTo(expectedResult);
    }
  }

  @Test
  public void errorResponse() throws IOException, InterruptedException {
    final FaultTolerantHttpClient httpClient = mockResponder(503, "");
    final HCaptchaClient client = new HCaptchaClient("fake", httpClient, mockConfig(true, 0.5));
    assertThrows(IOException.class, () -> client.verify(SITE_KEY, Action.CHALLENGE, TOKEN, null, USER_AGENT));
  }

  @Test
  public void invalidScore() throws IOException, InterruptedException {
    final FaultTolerantHttpClient httpClient = mockResponder(200, """
        {"success" : true, "score": 1.1}
        """);
    final HCaptchaClient client = new HCaptchaClient("fake", httpClient, mockConfig(true, 0.5));
    assertThat(client.verify(SITE_KEY, Action.CHALLENGE, TOKEN, null, USER_AGENT)).isEqualTo(AssessmentResult.invalid());
  }

  @Test
  public void badBody() throws IOException, InterruptedException {
    final FaultTolerantHttpClient httpClient = mockResponder(200, """
        {"success" : true,
        """);
    final HCaptchaClient client = new HCaptchaClient("fake", httpClient, mockConfig(true, 0.5));
    assertThrows(IOException.class, () -> client.verify(SITE_KEY, Action.CHALLENGE, TOKEN, null, USER_AGENT));
  }

  @Test
  public void disabled() throws IOException {
    final HCaptchaClient hc = new HCaptchaClient("fake", null, mockConfig(false, 0.5));
    assertTrue(Arrays.stream(Action.values()).map(hc::validSiteKeys).allMatch(Set::isEmpty));
  }

  @Test
  public void badSiteKey() throws IOException {
    final HCaptchaClient hc = new HCaptchaClient("fake", null, mockConfig(true, 0.5));
    for (Action action : Action.values()) {
      assertThat(hc.validSiteKeys(action)).contains(SITE_KEY);
      assertThat(hc.validSiteKeys(action)).doesNotContain("invalid");
    }
  }

  private static FaultTolerantHttpClient mockResponder(final int statusCode, final String jsonBody) {
    FaultTolerantHttpClient httpClient = mock(FaultTolerantHttpClient.class);
    @SuppressWarnings("unchecked") final HttpResponse<Object> httpResponse = mock(HttpResponse.class);

    when(httpResponse.body()).thenReturn(jsonBody);
    when(httpResponse.statusCode()).thenReturn(statusCode);

    when(httpClient.sendAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(httpResponse));
    return httpClient;
  }

  private static DynamicConfigurationManager<DynamicConfiguration> mockConfig(boolean enabled, double scoreFloor) {
    final DynamicCaptchaConfiguration config = new DynamicCaptchaConfiguration();
    config.setAllowHCaptcha(enabled);
    config.setScoreFloor(BigDecimal.valueOf(scoreFloor));
    config.setHCaptchaSiteKeys(Map.of(
        Action.REGISTRATION, Collections.singleton(SITE_KEY),
        Action.CHALLENGE, Collections.singleton(SITE_KEY)
    ));

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> m = mock(
        DynamicConfigurationManager.class);
    final DynamicConfiguration d = mock(DynamicConfiguration.class);
    when(m.getConfiguration()).thenReturn(d);
    when(d.getCaptchaConfiguration()).thenReturn(config);
    return m;
  }

}
