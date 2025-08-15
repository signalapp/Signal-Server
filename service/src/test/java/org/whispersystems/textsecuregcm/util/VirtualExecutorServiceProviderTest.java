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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.glassfish.jersey.server.ManagedAsync;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DropwizardExtensionsSupport.class)
class VirtualExecutorServiceProviderTest {

  private final TestController testController = new TestController();
  private final ResourceExtension resources = ResourceExtension.builder()
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addProvider(new VirtualExecutorServiceProvider( "virtual-thread-", 2))
      .addResource(testController)
      .build();

  @AfterEach
  void setUp() {
    testController.release();
  }

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
  public void testConcurrencyLimit() throws InterruptedException, TimeoutException {
    final BlockingQueue<Response> responses = new LinkedBlockingQueue<>();
    final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    for (int i = 0; i < 3; i++) {
      executor.submit(() -> responses.offer(resources.getJerseyTest().target("/v1/test/await").request().get()));
    }
    final Response rejectedResponse = responses.poll(10, TimeUnit.SECONDS);
    assertThat(rejectedResponse).isNotNull().extracting(Response::getStatus).isEqualTo(500);

    assertThat(responses.isEmpty()).isTrue();
    assertThat(testController.release()).isEqualTo(2);
    assertThat(responses.poll(1, TimeUnit.SECONDS)).isNotNull().extracting(Response::getStatus).isEqualTo(200);
    assertThat(responses.poll(1, TimeUnit.SECONDS)).isNotNull().extracting(Response::getStatus).isEqualTo(200);
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
    private List<CountDownLatch> latches = new ArrayList<>();

    @GET
    @Path("/managed-async")
    @ManagedAsync
    public Response managedAsync() {
      return Response.ok().entity(Thread.currentThread().getName()).build();
    }

    @GET
    @Path("/await")
    @ManagedAsync
    public Response await() throws InterruptedException {
      final CountDownLatch latch = new CountDownLatch(1);
      synchronized (this) {
        latches.add(latch);
      }
      latch.await();
      return Response.ok().build();
    }

    @GET
    @Path("/unmanaged")
    public Response unmanaged() {
      return Response.ok().entity(Thread.currentThread().getName()).build();
    }

    synchronized int release() {
      final Iterator<CountDownLatch> iterator = latches.iterator();
      int count;
      for (count = 0; iterator.hasNext(); count++) {
        iterator.next().countDown();
        iterator.remove();
      }
      return count;
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
