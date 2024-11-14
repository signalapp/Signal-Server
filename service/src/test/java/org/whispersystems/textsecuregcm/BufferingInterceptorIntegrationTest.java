package org.whispersystems.textsecuregcm;

import static org.assertj.core.api.Assertions.assertThat;

import io.dropwizard.core.Application;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.jetty.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.glassfish.jersey.server.ManagedAsync;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.util.BufferingInterceptor;
import org.whispersystems.textsecuregcm.util.VirtualExecutorServiceProvider;

@ExtendWith(DropwizardExtensionsSupport.class)
public class BufferingInterceptorIntegrationTest {
  private static final DropwizardAppExtension<Configuration> DROPWIZARD_APP_EXTENSION =
      new DropwizardAppExtension<>(TestApplication.class);

  public static class TestApplication extends Application<Configuration> {

    @Override
    public void run(final Configuration configuration, final Environment environment) throws Exception {
      final TestController testController = new TestController();
      environment.jersey().register(testController);
      environment.jersey().register(new BufferingInterceptor());
      environment.jersey().register(new VirtualExecutorServiceProvider("virtual-thread-"));
      JettyWebSocketServletContainerInitializer.configure(environment.getApplicationContext(), null);
    }
  }

  @Test
  public void testVirtual() {
    final Response response = DROPWIZARD_APP_EXTENSION.client()
        .target("http://127.0.0.1:%d/test/virtual/8".formatted(DROPWIZARD_APP_EXTENSION.getLocalPort()))
        .request().get();
    assertThat(response.getHeaders().getFirst(HttpHeaders.CONTENT_LENGTH)).isEqualTo("8");
  }

  @Test
  public void testPlatform() {
    final Response response = DROPWIZARD_APP_EXTENSION.client()
        .target("http://127.0.0.1:%d/test/platform/8".formatted(DROPWIZARD_APP_EXTENSION.getLocalPort()))
        .request().get();
    assertThat(response.getHeaders().getFirst(HttpHeaders.CONTENT_LENGTH)).isEqualTo("8");

  }

  @Path("/test")
  public static class TestController {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/virtual/{size}")
    @ManagedAsync
    public String getVirtual(@PathParam("size") int size) {
      return RandomStringUtils.secure().nextAscii(size);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/platform/{size}")
    public String getPlatform(@PathParam("size") int size) {
      return RandomStringUtils.secure().nextAscii(size);
    }
  }
}
