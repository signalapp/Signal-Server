/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;
import io.dropwizard.core.Application;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import jakarta.servlet.DispatcherType;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoServiceGrpc;
import org.whispersystems.textsecuregcm.grpc.EchoServiceImpl;
import org.whispersystems.textsecuregcm.grpc.GrpcTestUtils;
import org.whispersystems.textsecuregcm.grpc.MockRequestAttributesInterceptor;
import org.whispersystems.textsecuregcm.grpc.RequestAttributes;
import org.whispersystems.textsecuregcm.util.InetAddressRange;

@ExtendWith(DropwizardExtensionsSupport.class)
class ExternalRequestFilterTest {

  @Nested
  class Allowed extends TestCase {

    @Override
    DropwizardTestSupport<TestConfiguration> getTestSupport() {
      return new DropwizardTestSupport<>(TestApplication.class, getConfiguration());
    }

    @Override
    int getExpectedHttpStatus() {
      return 200;
    }

    @Override
    Status getExpectedGrpcStatus() {
      return Status.OK;
    }

    @Override
    TestConfiguration getConfiguration() {
      return new TestConfiguration() {
        @Override
        public Set<InetAddressRange> getPermittedRanges() {
          return Set.of(new InetAddressRange("127.0.0.0/8"));
        }
      };
    }

  }

  @Nested
  class Blocked extends TestCase {

    @Override
    DropwizardTestSupport<TestConfiguration> getTestSupport() {
      return new DropwizardTestSupport<>(TestApplication.class, getConfiguration());
    }

    @Override
    int getExpectedHttpStatus() {
      return 404;
    }

    @Override
    Status getExpectedGrpcStatus() {
      return Status.NOT_FOUND;
    }

    @Override
    TestConfiguration getConfiguration() {
      return new TestConfiguration() {
        @Override
        public Set<InetAddressRange> getPermittedRanges() {
          return Set.of(new InetAddressRange("10.0.0.0/8"));
        }
      };
    }
  }

  abstract static class TestCase {

    abstract DropwizardTestSupport<TestConfiguration> getTestSupport();

    abstract TestConfiguration getConfiguration();

    abstract int getExpectedHttpStatus();

    abstract Status getExpectedGrpcStatus();

    private Server testServer;
    private ManagedChannel channel;

    @Nested
    class Http {

      private final DropwizardAppExtension<TestConfiguration> DROPWIZARD_APP_EXTENSION =
          new DropwizardAppExtension<>(getTestSupport());

      @Test
      void testRestricted() {
        Client client = DROPWIZARD_APP_EXTENSION.client();

        try (Response response = client.target(
                "http://localhost:%s/test/restricted".formatted(DROPWIZARD_APP_EXTENSION.getLocalPort()))
            .request()
            .get()) {

          assertEquals(getExpectedHttpStatus(), response.getStatus());
        }
      }

      @Test
      void testOpen() {

        Client client = DROPWIZARD_APP_EXTENSION.client();

        try (Response response = client.target(
                "http://localhost:%s/test/open".formatted(DROPWIZARD_APP_EXTENSION.getLocalPort()))
            .request()
            .get()) {

          assertEquals(200, response.getStatus());
        }

      }
    }

    @Nested
    class Grpc {

      @BeforeEach
      void setUp() throws Exception {
        final MockRequestAttributesInterceptor mockRequestAttributesInterceptor = new MockRequestAttributesInterceptor();
        mockRequestAttributesInterceptor.setRequestAttributes(new RequestAttributes(InetAddresses.forString("127.0.0.1"), null, null));

        testServer = InProcessServerBuilder.forName("ExternalRequestFilterTest")
            .directExecutor()
            .addService(new EchoServiceImpl())
            .intercept(new ExternalRequestFilter(getConfiguration().getPermittedRanges(),
                Set.of("org.signal.chat.rpc.EchoService/echo2")))
            .intercept(mockRequestAttributesInterceptor)
            .build()
            .start();

        channel = InProcessChannelBuilder.forName("ExternalRequestFilterTest")
            .directExecutor()
            .build();
      }

      @Test
      void testBlocked() {
        final EchoServiceGrpc.EchoServiceBlockingStub client = EchoServiceGrpc.newBlockingStub(channel);

        final String text = "0123456789";
        final EchoRequest req = EchoRequest.newBuilder().setPayload(ByteString.copyFromUtf8(text)).build();

        final Status expectedGrpcStatus = getExpectedGrpcStatus();
        if (Status.Code.OK == expectedGrpcStatus.getCode()) {
          assertEquals(text, client.echo2(req).getPayload().toStringUtf8());
        } else {
          GrpcTestUtils.assertStatusException(expectedGrpcStatus, () -> client.echo2(req));
        }
      }

      @Test
      void testOpen() {
        final EchoServiceGrpc.EchoServiceBlockingStub client = EchoServiceGrpc.newBlockingStub(channel);

        final String text = "0123456789";
        final EchoRequest req = EchoRequest.newBuilder().setPayload(ByteString.copyFromUtf8(text)).build();

        assertEquals(text, client.echo(req).getPayload().toStringUtf8());
      }

      @AfterEach
      void tearDown() throws Exception {

        testServer.shutdownNow()
            .awaitTermination(10, TimeUnit.SECONDS);
      }
    }

    @Path("/test")
    public static class Controller {

      @GET
      @Path("/restricted")
      public Response restricted() {
        return Response.ok().build();
      }

      @GET
      @Path("/open")
      public Response open() {
        return Response.ok().build();
      }
    }

    public static class TestApplication extends Application<TestConfiguration> {

      @Override
      public void run(final TestConfiguration configuration, final Environment environment) throws Exception {

        environment.jersey().register(new Controller());
        environment.servlets()
            .addFilter("ExternalRequestFilter",
                new ExternalRequestFilter(configuration.getPermittedRanges(),
                    Collections.emptySet()))
            .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, "/test/restricted");
      }
    }

    public abstract static class TestConfiguration extends Configuration {

      public abstract Set<InetAddressRange> getPermittedRanges();
    }
  }

}
