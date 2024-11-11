/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import java.security.Principal;
import org.glassfish.jersey.server.ManagedAsync;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DropwizardExtensionsSupport.class)
class VirtualExecutorServiceProviderTest {

  private static final ResourceExtension resources = ResourceExtension.builder()
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addProvider(new VirtualExecutorServiceProvider("virtual-thread-"))
      .addResource(new TestController())
      .build();

  @Test
  public void testManagedAsyncThread() {
    final Response response = resources.getJerseyTest()
        .target("/v1/test/managed-async")
        .request()
        .get();
    String threadName = response.readEntity(String.class);
    assertThat(threadName).startsWith("virtual-thread-");
  }

  @Test
  public void testUnmanagedThread() {
    final Response response = resources.getJerseyTest()
        .target("/v1/test/unmanaged")
        .request()
        .get();
    String threadName = response.readEntity(String.class);
    assertThat(threadName).doesNotContain("virtual-thread-");
  }

  @Path("/v1/test")
  public static class TestController {

    @GET
    @Path("/managed-async")
    @ManagedAsync
    public Response managedAsync() {
      return Response.ok().entity(Thread.currentThread().getName()).build();
    }

    @GET
    @Path("/unmanaged")
    public Response unmanaged() {
      return Response.ok().entity(Thread.currentThread().getName()).build();
    }

  }

  public static class TestPrincipal implements Principal {

    private final String name;

    private TestPrincipal(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }
  }
}
