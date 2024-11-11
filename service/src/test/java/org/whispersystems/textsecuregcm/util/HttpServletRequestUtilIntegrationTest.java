/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.dropwizard.core.Application;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Context;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.jetty.util.HostPort;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(DropwizardExtensionsSupport.class)
class HttpServletRequestUtilIntegrationTest {

  private static final String PATH = "/test";

  // The Grizzly test container does not match the Jetty container used in real deployments, and JettyTestContainerFactory
  // in jersey-test-framework-provider-jetty doesn’t easily support @Context HttpServletRequest, so this test runs a
  // full Jetty server in a separate process
  private final DropwizardAppExtension<Configuration> EXTENSION = new DropwizardAppExtension<>(TestApplication.class);

  @ParameterizedTest
  @ValueSource(strings = {"127.0.0.1", "0:0:0:0:0:0:0:1"})
  void test(String ip) throws Exception {
    final Set<String> addresses = Arrays.stream(InetAddress.getAllByName("localhost"))
        .map(InetAddress::getHostAddress)
        .collect(Collectors.toSet());

    assumeTrue(addresses.contains(ip), String.format("localhost does not resolve to %s", ip));

    Client client = EXTENSION.client();

    final TestResponse response = client.target(
            String.format("http://%s:%d%s", HostPort.normalizeHost(ip), EXTENSION.getLocalPort(), PATH))
        .request("application/json")
        .get(TestResponse.class);

    assertEquals(ip, response.remoteAddress());
  }

  @Path(PATH)
  public static class TestController {

    @GET
    public TestResponse get(@Context HttpServletRequest request) {

      return new TestResponse(HttpServletRequestUtil.getRemoteAddress(request));
    }

  }

  public record TestResponse(String remoteAddress) {

  }

  public static class TestApplication extends Application<Configuration> {

    @Override
    public void run(final Configuration configuration, final Environment environment) throws Exception {
      environment.jersey().register(new TestController());
    }
  }
}
