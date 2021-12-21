package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import io.micrometer.core.instrument.Metrics;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.appconfig.AppConfigClient;
import software.amazon.awssdk.services.appconfig.model.GetConfigurationRequest;
import software.amazon.awssdk.services.appconfig.model.GetConfigurationResponse;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class DynamicConfigurationManager<T> {

  private final String application;
  private final String environment;
  private final String configurationName;
  private final String clientId;
  private final AppConfigClient appConfigClient;

  private final Class<T> configurationClass;

  private final AtomicReference<T> configuration = new AtomicReference<>();

  private GetConfigurationResponse lastConfigResult;

  private boolean initialized = false;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .registerModule(new JavaTimeModule());

  private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

  private static final String ERROR_COUNTER_NAME = name(DynamicConfigurationManager.class, "error");
  private static final String ERROR_TYPE_TAG_NAME = "type";
  private static final String CONFIG_CLASS_TAG_NAME = "configClass";

  private static final Logger logger = LoggerFactory.getLogger(DynamicConfigurationManager.class);

  public DynamicConfigurationManager(String application, String environment, String configurationName,
      Class<T> configurationClass) {
    this(AppConfigClient.builder()
            .overrideConfiguration(ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofMillis(10000))
                .apiCallAttemptTimeout(Duration.ofMillis(10000)).build())
            .build(),
        application, environment, configurationName, UUID.randomUUID().toString(), configurationClass);
  }

  @VisibleForTesting
  DynamicConfigurationManager(AppConfigClient appConfigClient, String application, String environment,
      String configurationName, String clientId, Class<T> configurationClass) {
    this.appConfigClient = appConfigClient;
    this.application = application;
    this.environment = environment;
    this.configurationName = configurationName;
    this.clientId = clientId;
    this.configurationClass = configurationClass;
  }

  public T getConfiguration() {
    synchronized (this) {
      while (!initialized) {
        Util.wait(this);
      }
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

  private Optional<T> retrieveDynamicConfiguration() throws JsonProcessingException {
    final String previousVersion = lastConfigResult != null ? lastConfigResult.configurationVersion() : null;

    try {
      lastConfigResult = appConfigClient.getConfiguration(GetConfigurationRequest.builder()
          .application(application)
          .environment(environment)
          .configuration(configurationName)
          .clientId(clientId)
          .clientConfigurationVersion(previousVersion)
          .build());
    } catch (final RuntimeException e) {
      Metrics.counter(ERROR_COUNTER_NAME, ERROR_TYPE_TAG_NAME, "fetch").increment();
      throw e;
    }

    final Optional<T> maybeDynamicConfiguration;

    if (!StringUtils.equals(lastConfigResult.configurationVersion(), previousVersion)) {
      logger.info("Received new config version: {}", lastConfigResult.configurationVersion());

      try {
        maybeDynamicConfiguration =
            parseConfiguration(
                StandardCharsets.UTF_8.decode(lastConfigResult.content().asByteBuffer().asReadOnlyBuffer()).toString(),
                configurationClass);
      } catch (final JsonProcessingException e) {
        Metrics.counter(ERROR_COUNTER_NAME,
            ERROR_TYPE_TAG_NAME, "parse",
            CONFIG_CLASS_TAG_NAME, configurationClass.getName()).increment();

        throw e;
      }
    } else {
      // No change since last version
      maybeDynamicConfiguration = Optional.empty();
    }

    return maybeDynamicConfiguration;
  }

  @VisibleForTesting
  public static <T> Optional<T> parseConfiguration(final String configurationYaml, final Class<T> configurationClass)
      throws JsonProcessingException {
    final T configuration = OBJECT_MAPPER.readValue(configurationYaml, configurationClass);
    final Set<ConstraintViolation<T>> violations = VALIDATOR.validate(configuration);

    final Optional<T> maybeDynamicConfiguration;

    if (violations.isEmpty()) {
      maybeDynamicConfiguration = Optional.of(configuration);
    } else {
      logger.warn("Failed to validate configuration: {}", violations);
      maybeDynamicConfiguration = Optional.empty();
    }

    return maybeDynamicConfiguration;
  }

  private T retrieveInitialDynamicConfiguration() {
    for (;;) {
      try {
        final Optional<T> maybeDynamicConfiguration = retrieveDynamicConfiguration();

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
