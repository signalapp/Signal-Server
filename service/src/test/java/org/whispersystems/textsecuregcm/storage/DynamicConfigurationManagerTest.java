package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.appconfigdata.AppConfigDataClient;
import software.amazon.awssdk.services.appconfigdata.model.GetLatestConfigurationRequest;
import software.amazon.awssdk.services.appconfigdata.model.GetLatestConfigurationResponse;
import software.amazon.awssdk.services.appconfigdata.model.StartConfigurationSessionRequest;
import software.amazon.awssdk.services.appconfigdata.model.StartConfigurationSessionResponse;

class DynamicConfigurationManagerTest {

  private static final SdkBytes VALID_CONFIG = SdkBytes.fromUtf8String("""
      test: true
      captcha:
        scoreFloor: 1.0
      """);

  private DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private AppConfigDataClient appConfig;
  private StartConfigurationSessionRequest startConfigurationSession;

  @BeforeEach
  void setup() {
    this.appConfig = mock(AppConfigDataClient.class);
    this.dynamicConfigurationManager = new DynamicConfigurationManager<>(
        appConfig, "foo", "bar", "baz", DynamicConfiguration.class);
    this.startConfigurationSession = StartConfigurationSessionRequest.builder()
        .applicationIdentifier("foo")
        .environmentIdentifier("bar")
        .configurationProfileIdentifier("baz")
        .build();
  }

  @Test
  void testGetInitialConfig() {
    when(appConfig.startConfigurationSession(startConfigurationSession))
        .thenReturn(StartConfigurationSessionResponse.builder()
            .initialConfigurationToken("initial")
            .build());

    // call with initial token will return a real config
    when(appConfig.getLatestConfiguration(GetLatestConfigurationRequest.builder()
        .configurationToken("initial").build()))
        .thenReturn(GetLatestConfigurationResponse.builder()
            .configuration(VALID_CONFIG)
            .nextPollConfigurationToken("next").build());

    // subsequent config calls will return empty (no update)
    when(appConfig.getLatestConfiguration(GetLatestConfigurationRequest.builder().
        configurationToken("next").build()))
        .thenReturn(GetLatestConfigurationResponse.builder()
            .configuration(SdkBytes.fromUtf8String(""))
            .nextPollConfigurationToken("next").build());

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      dynamicConfigurationManager.start();
      assertThat(dynamicConfigurationManager.getConfiguration()).isNotNull();
    });
  }

  @Test
  void testBadConfig() {
    when(appConfig.startConfigurationSession(startConfigurationSession))
        .thenReturn(StartConfigurationSessionResponse.builder()
            .initialConfigurationToken("initial")
            .build());

    // call with initial token will return a bad config
    when(appConfig.getLatestConfiguration(GetLatestConfigurationRequest.builder()
        .configurationToken("initial").build()))
        .thenReturn(GetLatestConfigurationResponse.builder()
            .configuration(SdkBytes.fromUtf8String("zzz"))
            .nextPollConfigurationToken("goodconfig").build());

    // next config call will return a good config
    when(appConfig.getLatestConfiguration(GetLatestConfigurationRequest.builder().
        configurationToken("goodconfig").build()))
        .thenReturn(GetLatestConfigurationResponse.builder()
            .configuration(VALID_CONFIG)
            .nextPollConfigurationToken("next").build());

    // all subsequent config calls will return an empty config (no update)
    when(appConfig.getLatestConfiguration(GetLatestConfigurationRequest.builder().
        configurationToken("next").build()))
        .thenReturn(GetLatestConfigurationResponse.builder()
            .configuration(SdkBytes.fromUtf8String(""))
            .nextPollConfigurationToken("next").build());

    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      dynamicConfigurationManager.start();
      assertThat(dynamicConfigurationManager.getConfiguration()).isNotNull();
    });
  }

  @Test
  void testGetConfigMultiple() {
    when(appConfig.startConfigurationSession(startConfigurationSession))
        .thenReturn(StartConfigurationSessionResponse.builder()
            .initialConfigurationToken("0")
            .build());

    // initial config
    when(appConfig.getLatestConfiguration(GetLatestConfigurationRequest.builder().
        configurationToken("0").build()))
        .thenReturn(GetLatestConfigurationResponse.builder()
            .configuration(VALID_CONFIG)
            .nextPollConfigurationToken("1").build());

    // config update with a real config
    when(appConfig.getLatestConfiguration(GetLatestConfigurationRequest.builder().
        configurationToken("1").build()))
        .thenReturn(GetLatestConfigurationResponse.builder()
            .configuration(SdkBytes.fromUtf8String("""
                featureFlags:
                  - testFlag
                captcha:
                  scoreFloor: 1.0
                """))
            .nextPollConfigurationToken("2").build());

    // all subsequent are no update
    when(appConfig.getLatestConfiguration(GetLatestConfigurationRequest.builder().
        configurationToken("2").build()))
        .thenReturn(GetLatestConfigurationResponse.builder()
            .configuration(SdkBytes.fromUtf8String(""))
            .nextPollConfigurationToken("2").build());

    // the internal waiting done by dynamic configuration manager catches the InterruptedException used
    // by JUnit’s @Timeout, so we use assertTimeoutPreemptively
    assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
      // we should eventually get the updated config (or the test will timeout)
      dynamicConfigurationManager.start();
      while (dynamicConfigurationManager.getConfiguration().getActiveFeatureFlags().isEmpty()) {
        Thread.sleep(100);
      }
      assertThat(dynamicConfigurationManager.getConfiguration().getActiveFeatureFlags()).containsExactly("testFlag");
    });

  }
}
