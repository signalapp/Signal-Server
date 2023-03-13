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
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicCaptchaConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class HCaptchaClientTest {

  private static final String SITE_KEY = "site-key";
  private static final String TOKEN = "token";


  static Stream<Arguments> captchaProcessed() {
    return Stream.of(
        Arguments.of(true, 0.6f, true),
        Arguments.of(false, 0.6f, false),
        Arguments.of(true, 0.4f, false),
        Arguments.of(false, 0.4f, false)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void captchaProcessed(final boolean success, final float score, final boolean expectedResult)
      throws IOException, InterruptedException {

    final HttpClient client = mockResponder(200, String.format("""
            {
              "success": %b,
              "score": %f,
              "score-reasons": ["great job doing this captcha"]
            }
            """,
        success, 1 - score)); // hCaptcha scores are inverted compared to recaptcha scores. (low score is good)

    final AssessmentResult result = new HCaptchaClient("fake", client, mockConfig(true, 0.5))
        .verify(SITE_KEY, Action.CHALLENGE, TOKEN, null);
    if (!success) {
      assertThat(result).isEqualTo(AssessmentResult.invalid());
    } else {
      assertThat(result)
          .isEqualTo(new AssessmentResult(expectedResult, AssessmentResult.scoreString(score)));
    }
  }

  @Test
  public void errorResponse() throws IOException, InterruptedException {
    final HttpClient httpClient = mockResponder(503, "");
    final HCaptchaClient client = new HCaptchaClient("fake", httpClient, mockConfig(true, 0.5));
    assertThrows(IOException.class, () -> client.verify(SITE_KEY, Action.CHALLENGE, TOKEN, null));
  }

  @Test
  public void invalidScore() throws IOException, InterruptedException {
    final HttpClient httpClient = mockResponder(200, """
        {"success" : true, "score": 1.1}
        """);
    final HCaptchaClient client = new HCaptchaClient("fake", httpClient, mockConfig(true, 0.5));
    assertThat(client.verify(SITE_KEY, Action.CHALLENGE, TOKEN, null)).isEqualTo(AssessmentResult.invalid());
  }

  @Test
  public void badBody() throws IOException, InterruptedException {
    final HttpClient httpClient = mockResponder(200, """
        {"success" : true,
        """);
    final HCaptchaClient client = new HCaptchaClient("fake", httpClient, mockConfig(true, 0.5));
    assertThrows(IOException.class, () -> client.verify(SITE_KEY, Action.CHALLENGE, TOKEN, null));
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

  private static HttpClient mockResponder(final int statusCode, final String jsonBody)
      throws IOException, InterruptedException {
    HttpClient httpClient = mock(HttpClient.class);
    @SuppressWarnings("unchecked") final HttpResponse<Object> httpResponse = mock(HttpResponse.class);

    when(httpResponse.body()).thenReturn(jsonBody);
    when(httpResponse.statusCode()).thenReturn(statusCode);

    when(httpClient.send(any(), any())).thenReturn(httpResponse);
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
