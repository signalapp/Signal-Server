package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalAnswers;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.s3.S3ObjectMonitor;

class DynamicConfigurationManagerTest {

  private static final byte[] VALID_CONFIG = """
    test: true
    captcha:
      scoreFloor: 1.0
    """.getBytes();
  private static final ExecutorService BACKGROUND_THREAD = Executors.newSingleThreadExecutor();

  private DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private S3ObjectMonitor configMonitor;

  @BeforeEach
  void setup() {
    this.configMonitor = mock(S3ObjectMonitor.class);
    this.dynamicConfigurationManager = new DynamicConfigurationManager<>(configMonitor, DynamicConfiguration.class);
  }

  @Test
  void testGetInitialConfig() {
    // supply real config on start, then never send updates
    doAnswer(AdditionalAnswers.<Consumer<InputStream>>answerVoid(cb -> cb.accept(new ByteArrayInputStream(VALID_CONFIG))))
        .when(configMonitor).start(any());

    assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
      dynamicConfigurationManager.start();
      assertThat(dynamicConfigurationManager.getConfiguration()).isNotNull();
    });
  }

  @Test
  void testBadConfig() {
    // supply a bad config, then wait for the test to signal, then supply a good config
    doAnswer(AdditionalAnswers.<Consumer<InputStream>>answerVoid(cb -> {
              cb.accept(new ByteArrayInputStream("zzz".getBytes()));
              BACKGROUND_THREAD.submit(() -> cb.accept(new ByteArrayInputStream(VALID_CONFIG)));
            })).when(configMonitor).start(any());

    assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
          dynamicConfigurationManager.start();
          assertThat(dynamicConfigurationManager.getConfiguration()).isNotNull();
    });
  }

  @Test
  void testGetConfigMultiple() {
    final CyclicBarrier barrier = new CyclicBarrier(2);
    // supply an initial config, wait for the test to signal, then supply a distinct good config
    doAnswer(AdditionalAnswers.<Consumer<InputStream>>answerVoid(cb -> {
              cb.accept(new ByteArrayInputStream(VALID_CONFIG));
              BACKGROUND_THREAD.submit(() -> {
                    try {
                      barrier.await(); // wait for initial config to be consumed
                      cb.accept(
                          new ByteArrayInputStream("""
                              experiments:
                                test:
                                  enrollmentPercentage: 50
                              captcha:
                                scoreFloor: 1.0
                              """.getBytes()));
                      barrier.await(); // signal availability of new config
                    } catch (InterruptedException | BrokenBarrierException e) {}
                  });
            })).when(configMonitor).start(any());

    // the internal waiting done by dynamic configuration manager catches the InterruptedException used
    // by JUnit’s @Timeout, so we use assertTimeoutPreemptively
    assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
      dynamicConfigurationManager.start();
      DynamicConfiguration config = dynamicConfigurationManager.getConfiguration();
      assertThat(config).isNotNull();
      assertThat(config.getExperimentEnrollmentConfiguration("test")).isEmpty();
      barrier.await();          // signal consumption of initial config
      barrier.await();          // wait for availability of new config
      config = dynamicConfigurationManager.getConfiguration();
      assertThat(config).isNotNull();
      assertThat(config.getExperimentEnrollmentConfiguration("test")).isNotEmpty();
      assertThat(config.getExperimentEnrollmentConfiguration("test").get().getEnrollmentPercentage())
          .isEqualTo(50);
    });
  }

}
