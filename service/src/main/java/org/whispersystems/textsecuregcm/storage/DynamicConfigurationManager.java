package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.appconfigdata.AppConfigDataClient;
import software.amazon.awssdk.services.appconfigdata.model.GetLatestConfigurationRequest;
import software.amazon.awssdk.services.appconfigdata.model.GetLatestConfigurationResponse;
import software.amazon.awssdk.services.appconfigdata.model.StartConfigurationSessionRequest;
import software.amazon.awssdk.services.appconfigdata.model.StartConfigurationSessionResponse;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class DynamicConfigurationManager<T> {

  private final String application;
  private final String environment;
  private final String configurationName;
  private final AppConfigDataClient appConfigClient;
  private final Class<T> configurationClass;

  // Set on initial config fetch
  private final AtomicReference<T> configuration = new AtomicReference<>();
  private String configurationToken = null;
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
    this(AppConfigDataClient
            .builder()
            .overrideConfiguration(ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofSeconds(10))
                .apiCallAttemptTimeout(Duration.ofSeconds(10)).build())
            .build(),
        application, environment, configurationName, configurationClass);
  }

  @VisibleForTesting
  DynamicConfigurationManager(AppConfigDataClient appConfigClient, String application, String environment,
      String configurationName, Class<T> configurationClass) {
    this.appConfigClient = appConfigClient;
    this.application = application;
    this.environment = environment;
    this.configurationName = configurationName;
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
        } catch (Exception e) {
          logger.warn("Error retrieving dynamic configuration", e);
        }

        Util.sleep(5000);
      }
    }, "DynamicConfigurationManagerWorker");

    workerThread.setDaemon(true);
    workerThread.start();
  }

  private Optional<T> retrieveDynamicConfiguration() throws JsonProcessingException {
    if (configurationToken == null) {
        logger.error("Invalid configuration token, will not be able to fetch configuration updates");
    }
    GetLatestConfigurationResponse latestConfiguration;
    try {
      latestConfiguration = appConfigClient.getLatestConfiguration(GetLatestConfigurationRequest.builder()
          .configurationToken(configurationToken)
          .build());
      // token to use in the next fetch
      configurationToken = latestConfiguration.nextPollConfigurationToken();
      logger.debug("next token: {}", configurationToken);
    } catch (final RuntimeException e) {
      Metrics.counter(ERROR_COUNTER_NAME, ERROR_TYPE_TAG_NAME, "fetch").increment();
      throw e;
    }

    if (!latestConfiguration.configuration().asByteBuffer().hasRemaining()) {
      // empty configuration means nothing has changed
      return Optional.empty();
    }
    logger.info("Received new config of length {}, next configuration token: {}",
        latestConfiguration.configuration().asByteBuffer().remaining(),
        configurationToken);

    try {
      return parseConfiguration(latestConfiguration.configuration().asUtf8String(), configurationClass);
    } catch (final JsonProcessingException e) {
      Metrics.counter(ERROR_COUNTER_NAME,
          ERROR_TYPE_TAG_NAME, "parse",
          CONFIG_CLASS_TAG_NAME, configurationClass.getName()).increment();
      throw e;
    }
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
        if (configurationToken == null) {
          // first time around, start the configuration session
          final StartConfigurationSessionResponse startResponse = appConfigClient
              .startConfigurationSession(StartConfigurationSessionRequest.builder()
                  .applicationIdentifier(application)
                  .environmentIdentifier(environment)
                  .configurationProfileIdentifier(configurationName).build());
          configurationToken = startResponse.initialConfigurationToken();
        }
        return retrieveDynamicConfiguration().orElseThrow(() -> new IllegalStateException("No initial configuration available"));
      } catch (Exception e) {
        logger.warn("Error retrieving initial dynamic configuration", e);
        Util.sleep(1000);
      }
    }
  }
}
