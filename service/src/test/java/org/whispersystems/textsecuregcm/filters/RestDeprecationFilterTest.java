/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.SecurityContext;
import java.net.URI;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import org.glassfish.jersey.server.ContainerRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.tests.util.FakeDynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.SystemMapper;

class RestDeprecationFilterTest {

  @Test
  void testNoConfig() throws Exception {
    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        new FakeDynamicConfigurationManager<>(new DynamicConfiguration());
    final ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(dynamicConfigurationManager);

    final RestDeprecationFilter filter = new RestDeprecationFilter(dynamicConfigurationManager, experimentEnrollmentManager);

    final SecurityContext securityContext = mock(SecurityContext.class);
    when(securityContext.getUserPrincipal()).thenReturn(new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID, Instant.now()));
    final ContainerRequest req = new ContainerRequest(null, new URI("/some/uri"), "GET", securityContext, null, null);
    req.getHeaders().add(HttpHeaders.USER_AGENT, "Signal-Android/100.0.0");

    filter.filter(req);
  }

  @Test
  void testOldClientAuthenticated() throws Exception {
    final DynamicConfiguration config = SystemMapper.yamlMapper().readValue(
        """
        restDeprecation:
          platforms:
            ANDROID:
              minimumRestFreeVersion: 200.0.0
        experiments:
          restDeprecation:
            uuidEnrollmentPercentage: 100
        """,
        DynamicConfiguration.class);
    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = new FakeDynamicConfigurationManager<>(config);
    final ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(dynamicConfigurationManager);

    final RestDeprecationFilter filter = new RestDeprecationFilter(dynamicConfigurationManager, experimentEnrollmentManager);

    final SecurityContext securityContext = mock(SecurityContext.class);
    when(securityContext.getUserPrincipal()).thenReturn(new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID, Instant.now()));
    final ContainerRequest req = new ContainerRequest(null, new URI("/some/uri"), "GET", securityContext, null, null);
    req.getHeaders().add(HttpHeaders.USER_AGENT, "Signal-Android/100.0.0");

    filter.filter(req);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99})
  void testOldClientUnauthenticated(int randomRoll) throws Exception {
    final DynamicConfiguration config = SystemMapper.yamlMapper().readValue(
        """
        restDeprecation:
          platforms:
            ANDROID:
              minimumRestFreeVersion: 200.0.0
              universalRolloutPercent: 50
        experiments:
          restDeprecation:
            uuidEnrollmentPercentage: 100
        """,
        DynamicConfiguration.class);
    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = new FakeDynamicConfigurationManager<>(config);
    final ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(dynamicConfigurationManager);
    final Random fakeRandom = mock(Random.class);
    when(fakeRandom.nextInt(anyInt())).thenReturn(randomRoll);

    final RestDeprecationFilter filter = new RestDeprecationFilter(dynamicConfigurationManager, experimentEnrollmentManager, () -> fakeRandom);

    final ContainerRequest req = new ContainerRequest(null, new URI("/some/uri"), "GET", null, null, null);
    req.getHeaders().add(HttpHeaders.USER_AGENT, "Signal-Android/100.0.0");

    filter.filter(req);
  }

  @Test
  void testBlockingAuthenticated() throws Exception {
    final DynamicConfiguration config = SystemMapper.yamlMapper().readValue(
        """
        restDeprecation:
          platforms:
            ANDROID:
              minimumRestFreeVersion: 10.10.10
        experiments:
          restDeprecation:
            enrollmentPercentage: 100
        """,
        DynamicConfiguration.class);
    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = new FakeDynamicConfigurationManager<>(config);
    final ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(dynamicConfigurationManager);

    final RestDeprecationFilter filter = new RestDeprecationFilter(dynamicConfigurationManager, experimentEnrollmentManager);

    final SecurityContext securityContext = mock(SecurityContext.class);
    when(securityContext.getUserPrincipal()).thenReturn(new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID, Instant.now()));
    final ContainerRequest req = new ContainerRequest(null, new URI("/some/path"), "GET", securityContext, null, null);

    req.getHeaders().putSingle(HttpHeaders.USER_AGENT, "Signal-Android/10.9.15");
    filter.filter(req);

    req.getHeaders().putSingle(HttpHeaders.USER_AGENT, "Signal-Android/10.10.9");
    filter.filter(req);

    req.getHeaders().putSingle(HttpHeaders.USER_AGENT, "Signal-Android/10.10.10");
    assertThrows(WebApplicationException.class, () -> filter.filter(req));

    req.getHeaders().putSingle(HttpHeaders.USER_AGENT, "Signal-Android/100.0.0");
    assertThrows(WebApplicationException.class, () -> filter.filter(req));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 10, 20, 30, 40, 50, 60, 69, 70, 71, 80, 90, 99})
  void testBlockingUnauthenticated(int randomRoll) throws Exception {
    final DynamicConfiguration config = SystemMapper.yamlMapper().readValue(
        """
        restDeprecation:
          platforms:
            ANDROID:
              minimumRestFreeVersion: 10.10.10
              universalRolloutPercent: 70
        """,
        DynamicConfiguration.class);
    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = new FakeDynamicConfigurationManager<>(config);
    final ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(dynamicConfigurationManager);
    final Random fakeRandom = mock(Random.class);
    when(fakeRandom.nextInt(anyInt())).thenReturn(randomRoll);

    final RestDeprecationFilter filter = new RestDeprecationFilter(dynamicConfigurationManager, experimentEnrollmentManager, () -> fakeRandom);

    final ContainerRequest req = new ContainerRequest(null, new URI("/some/path"), "GET", null, null, null);

    req.getHeaders().putSingle(HttpHeaders.USER_AGENT, "Signal-Android/10.9.15");
    filter.filter(req);

    req.getHeaders().putSingle(HttpHeaders.USER_AGENT, "Signal-Android/10.10.9");
    filter.filter(req);

    req.getHeaders().putSingle(HttpHeaders.USER_AGENT, "Signal-Android/10.10.10");
    if (randomRoll < 70) {
      assertThrows(WebApplicationException.class, () -> filter.filter(req));
    } else {
      filter.filter(req);
    }

    req.getHeaders().putSingle(HttpHeaders.USER_AGENT, "Signal-Android/100.0.0");
    if (randomRoll < 70) {
      assertThrows(WebApplicationException.class, () -> filter.filter(req));
    } else {
      filter.filter(req);
    }
  }

}
