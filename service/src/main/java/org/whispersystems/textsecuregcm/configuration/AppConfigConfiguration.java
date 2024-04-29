package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.ScheduledExecutorService;
import javax.validation.constraints.NotEmpty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@JsonTypeName("default")
public class AppConfigConfiguration implements DynamicConfigurationManagerFactory {

  @JsonProperty
  @NotEmpty
  private String application;

  @JsonProperty
  @NotEmpty
  private String environment;

  @JsonProperty
  @NotEmpty
  private String configuration;

  public String getApplication() {
    return application;
  }

  public String getEnvironment() {
    return environment;
  }

  public String getConfigurationName() {
    return configuration;
  }

  @Override
  public <T> DynamicConfigurationManager<T> build(Class<T> klazz, ScheduledExecutorService scheduledExecutorService,
      AwsCredentialsProvider awsCredentialsProvider) {
    return new DynamicConfigurationManager<>(application, environment, configuration, awsCredentialsProvider, klazz,
        scheduledExecutorService);
  }
}
