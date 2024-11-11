/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRemoteAddressFilterProvider;

@ExtendWith(DropwizardExtensionsSupport.class)
public class RateLimitedByIpTest {

  private static final String IP = "127.0.0.1";

  private static final Duration RETRY_AFTER = Duration.ofSeconds(100);


  @Path("/test")
  public static class Controller {
    @GET
    @Path("/strict")
    @RateLimitedByIp(RateLimiters.For.BACKUP_AUTH_CHECK)
    public Response strict() {
      return Response.ok().build();
    }

    @GET
    @Path("/loose")
    @RateLimitedByIp(value = RateLimiters.For.BACKUP_AUTH_CHECK, failOnUnresolvedIp = false)
    public Response loose() {
      return Response.ok().build();
    }
  }

  private static final RateLimiter RATE_LIMITER = mock(RateLimiter.class);

  private static final RateLimiters RATE_LIMITERS = MockUtils.buildMock(RateLimiters.class, rl ->
      when(rl.forDescriptor(eq(RateLimiters.For.BACKUP_AUTH_CHECK))).thenReturn(RATE_LIMITER));

  private static final ResourceExtension RESOURCES = ResourceExtension.builder()
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new Controller())
      .addProvider(new RateLimitByIpFilter(RATE_LIMITERS))
      .addProvider(new TestRemoteAddressFilterProvider(IP))
      .build();

  @Test
  public void testRateLimits() throws Exception {
    doNothing().when(RATE_LIMITER).validate(eq(IP));
    validateSuccess("/test/strict");
    doThrow(new RateLimitExceededException(RETRY_AFTER)).when(RATE_LIMITER).validate(eq(IP));
    validateFailure("/test/strict", RETRY_AFTER);
    doNothing().when(RATE_LIMITER).validate(eq(IP));
    validateSuccess("/test/strict");
    doThrow(new RateLimitExceededException(RETRY_AFTER)).when(RATE_LIMITER).validate(eq(IP));
    validateFailure("/test/strict", RETRY_AFTER);
  }

  private static void validateSuccess(final String path) {
    final Response response = RESOURCES.getJerseyTest()
        .target(path)
        .request()
        .get();

    assertEquals(200, response.getStatus());
  }

  private static void validateFailure(final String path, final Duration expectedRetryAfter) {
    final Response response = RESOURCES.getJerseyTest()
        .target(path)
        .request()
        .get();

    assertEquals(429, response.getStatus());
    assertEquals("" + expectedRetryAfter.getSeconds(), response.getHeaderString(HttpHeaders.RETRY_AFTER));
  }
}
