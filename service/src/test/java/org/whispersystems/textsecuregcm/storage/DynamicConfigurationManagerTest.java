package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.appconfig.AppConfigClient;
import software.amazon.awssdk.services.appconfig.model.GetConfigurationRequest;
import software.amazon.awssdk.services.appconfig.model.GetConfigurationResponse;

class DynamicConfigurationManagerTest {

  private DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private AppConfigClient             appConfig;

  @BeforeEach
  void setup() {
    this.appConfig                   = mock(AppConfigClient.class);
    this.dynamicConfigurationManager = new DynamicConfigurationManager<>(appConfig, "foo", "bar", "baz", "poof", DynamicConfiguration.class);
  }

  @Test
  void testGetConfig() {
    ArgumentCaptor<GetConfigurationRequest> captor = ArgumentCaptor.forClass(GetConfigurationRequest.class);
    when(appConfig.getConfiguration(captor.capture())).thenReturn(
        GetConfigurationResponse.builder().content(SdkBytes.fromByteArray("test: true".getBytes())).configurationVersion("1").build());

    dynamicConfigurationManager.start();

    assertThat(captor.getValue().application()).isEqualTo("foo");
    assertThat(captor.getValue().environment()).isEqualTo("bar");
    assertThat(captor.getValue().configuration()).isEqualTo("baz");
    assertThat(captor.getValue().clientId()).isEqualTo("poof");

    assertThat(dynamicConfigurationManager.getConfiguration()).isNotNull();
  }
}
