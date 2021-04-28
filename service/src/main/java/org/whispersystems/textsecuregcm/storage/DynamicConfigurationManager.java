package org.whispersystems.textsecuregcm.storage;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.appconfig.AmazonAppConfig;
import com.amazonaws.services.appconfig.AmazonAppConfigClient;
import com.amazonaws.services.appconfig.model.GetConfigurationRequest;
import com.amazonaws.services.appconfig.model.GetConfigurationResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.util.Util;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class DynamicConfigurationManager {

  private final String          application;
  private final String          environment;
  private final String          configurationName;
  private final String          clientId;
  private final AmazonAppConfig appConfigClient;

  private final AtomicReference<DynamicConfiguration>   configuration    = new AtomicReference<>();
  private final Logger                                  logger           = LoggerFactory.getLogger(DynamicConfigurationManager.class);

  private GetConfigurationResult lastConfigResult;

  private boolean initialized = false;

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .registerModule(new JavaTimeModule());

  public DynamicConfigurationManager(String application, String environment, String configurationName) {
    this(AmazonAppConfigClient.builder()
                              .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(10000).withRequestTimeout(10000))
                              .withCredentials(InstanceProfileCredentialsProvider.getInstance())
                              .build(),
         application, environment, configurationName, UUID.randomUUID().toString());
  }

  @VisibleForTesting
  public DynamicConfigurationManager(AmazonAppConfig appConfigClient, String application, String environment, String configurationName, String clientId) {
    this.appConfigClient   = appConfigClient;
    this.application       = application;
    this.environment       = environment;
    this.configurationName = configurationName;
    this.clientId          = clientId;
  }

  public DynamicConfiguration getConfiguration() {
    synchronized (this) {
      while (!initialized) Util.wait(this);
    }

    return configuration.get();
  }

  public void start() {
    configuration.set(retrieveInitialDynamicConfiguration());

    synchronized (this) {
      this.initialized = true;
      this.notifyAll();
    }

    final Thread workerThread = new Thread(() -> {
      while (true) {
        try {
          retrieveDynamicConfiguration().ifPresent(configuration::set);
        } catch (Throwable t) {
          logger.warn("Error retrieving dynamic configuration", t);
        }

        Util.sleep(5000);
      }
    }, "DynamicConfigurationManagerWorker");

    workerThread.setDaemon(true);
    workerThread.start();
  }

  private Optional<DynamicConfiguration> retrieveDynamicConfiguration() throws JsonProcessingException {
    final String previousVersion = lastConfigResult != null ? lastConfigResult.getConfigurationVersion() : null;

    lastConfigResult = appConfigClient.getConfiguration(new GetConfigurationRequest().withApplication(application)
                                                                                     .withEnvironment(environment)
                                                                                     .withConfiguration(configurationName)
                                                                                     .withClientId(clientId)
                                                                                     .withClientConfigurationVersion(previousVersion));

    final Optional<DynamicConfiguration> maybeDynamicConfiguration;

    if (!StringUtils.equals(lastConfigResult.getConfigurationVersion(), previousVersion)) {
      logger.info("Received new config version: {}", lastConfigResult.getConfigurationVersion());
      maybeDynamicConfiguration = Optional.of(OBJECT_MAPPER.readValue(StandardCharsets.UTF_8.decode(lastConfigResult.getContent().asReadOnlyBuffer()).toString(), DynamicConfiguration.class));
    } else {
      // No change since last version
      maybeDynamicConfiguration = Optional.empty();
    }

    return maybeDynamicConfiguration;
  }

  private DynamicConfiguration retrieveInitialDynamicConfiguration() {
    for (;;) {
      try {
        final Optional<DynamicConfiguration> maybeDynamicConfiguration = retrieveDynamicConfiguration();

        if (maybeDynamicConfiguration.isPresent()) {
          return maybeDynamicConfiguration.get();
        } else {
          throw new IllegalStateException("No initial configuration available");
        }
      } catch (Throwable t) {
        logger.warn("Error retrieving initial dynamic configuration", t);
        Util.sleep(1000);
      }
    }
  }
}
