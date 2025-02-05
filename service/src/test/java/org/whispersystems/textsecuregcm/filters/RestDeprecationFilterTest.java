/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.SecurityContext;
import java.net.URI;
import java.util.UUID;
import org.glassfish.jersey.server.ContainerRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.storage.Account;
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

    final Account account = new Account();
    account.setUuid(UUID.randomUUID());
    final SecurityContext securityContext = mock(SecurityContext.class);
    when(securityContext.getUserPrincipal()).thenReturn(new AuthenticatedDevice(account, new Device()));
    final ContainerRequest req = new ContainerRequest(null, new URI("/some/uri"), "GET", securityContext, null, null);
    req.getHeaders().add(HttpHeaders.USER_AGENT, "Signal-Android/100.0.0");

    filter.filter(req);
  }

  @Test
  void testOldClient() throws Exception {
    final DynamicConfiguration config = SystemMapper.yamlMapper().readValue(
        """
        minimumRestFreeVersion:
          ANDROID: 200.0.0
        experiments:
          restDeprecation:
            uuidEnrollmentPercentage: 100
            """,
        DynamicConfiguration.class);
    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = new FakeDynamicConfigurationManager<>(config);
    final ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(dynamicConfigurationManager);

    final RestDeprecationFilter filter = new RestDeprecationFilter(dynamicConfigurationManager, experimentEnrollmentManager);

    final Account account = new Account();
    account.setUuid(UUID.randomUUID());
    final SecurityContext securityContext = mock(SecurityContext.class);
    when(securityContext.getUserPrincipal()).thenReturn(new AuthenticatedDevice(account, new Device()));
    final ContainerRequest req = new ContainerRequest(null, new URI("/some/uri"), "GET", securityContext, null, null);
    req.getHeaders().add(HttpHeaders.USER_AGENT, "Signal-Android/100.0.0");

    filter.filter(req);
  }

  @Test
  void testBlocking() throws Exception {
    final DynamicConfiguration config = SystemMapper.yamlMapper().readValue(
        """
        minimumRestFreeVersion:
          ANDROID: 10.10.10
        experiments:
          restDeprecation:
            enrollmentPercentage: 100
            """,
        DynamicConfiguration.class);
    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = new FakeDynamicConfigurationManager<>(config);
    final ExperimentEnrollmentManager experimentEnrollmentManager = new ExperimentEnrollmentManager(dynamicConfigurationManager);

    final RestDeprecationFilter filter = new RestDeprecationFilter(dynamicConfigurationManager, experimentEnrollmentManager);

    final Account account = new Account();
    account.setUuid(UUID.randomUUID());
    final SecurityContext securityContext = mock(SecurityContext.class);
    when(securityContext.getUserPrincipal()).thenReturn(new AuthenticatedDevice(account, new Device()));
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

}
