package org.whispersystems.textsecuregcm.storage;

import com.amazonaws.services.appconfig.AmazonAppConfig;
import com.amazonaws.services.appconfig.model.GetConfigurationRequest;
import com.amazonaws.services.appconfig.model.GetConfigurationResult;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DynamicConfigurationManagerTest {

  private DynamicConfigurationManager dynamicConfigurationManager;
  private AmazonAppConfig             appConfig;

  @Before
  public void setup() {
    this.appConfig                   = mock(AmazonAppConfig.class);
    this.dynamicConfigurationManager = new DynamicConfigurationManager(appConfig, "foo", "bar", "baz", "poof");
  }

  @Test
  public void testGetConfig() {
    ArgumentCaptor<GetConfigurationRequest> captor = ArgumentCaptor.forClass(GetConfigurationRequest.class);
    when(appConfig.getConfiguration(captor.capture())).thenReturn(new GetConfigurationResult().withContent(ByteBuffer.wrap("test: true".getBytes()))
                                                                                              .withConfigurationVersion("1"));

    dynamicConfigurationManager.start();

    assertThat(captor.getValue().getApplication()).isEqualTo("foo");
    assertThat(captor.getValue().getEnvironment()).isEqualTo("bar");
    assertThat(captor.getValue().getConfiguration()).isEqualTo("baz");
    assertThat(captor.getValue().getClientId()).isEqualTo("poof");

    assertThat(dynamicConfigurationManager.getConfiguration()).isNotNull();
  }
}
