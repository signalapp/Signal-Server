/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.net.HttpHeaders;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.time.Duration;
import java.util.Optional;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
public class RateLimitedByIpTest {

  private static final String IP = "70.130.130.200";

  private static final String VALID_X_FORWARDED_FOR = "1.1.1.1," + IP;

  private static final String INVALID_X_FORWARDED_FOR = "1.1.1.1,";

  private static final Duration RETRY_AFTER = Duration.ofSeconds(100);

  private static final Duration RETRY_AFTER_INVALID_HEADER = RateLimitByIpFilter.INVALID_HEADER_EXCEPTION
      .getRetryDuration()
      .orElseThrow();


  @Path("/test")
  public static class Controller {
    @GET
    @Path("/strict")
    @RateLimitedByIp(RateLimiters.Handle.BACKUP_AUTH_CHECK)
    public Response strict() {
      return Response.ok().build();
    }

    @GET
    @Path("/loose")
    @RateLimitedByIp(value = RateLimiters.Handle.BACKUP_AUTH_CHECK, failOnUnresolvedIp = false)
    public Response loose() {
      return Response.ok().build();
    }
  }

  private static final RateLimiter RATE_LIMITER = Mockito.mock(RateLimiter.class);

  private static final RateLimiters RATE_LIMITERS = MockUtils.buildMock(RateLimiters.class, rl ->
      Mockito.when(rl.byHandle(Mockito.eq(RateLimiters.Handle.BACKUP_AUTH_CHECK))).thenReturn(Optional.of(RATE_LIMITER)));

  private static final ResourceExtension RESOURCES = ResourceExtension.builder()
      .setMapper(SystemMapper.getMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new Controller())
      .addProvider(new RateLimitByIpFilter(RATE_LIMITERS))
      .build();

  @Test
  public void testRateLimits() throws Exception {
    Mockito.doNothing().when(RATE_LIMITER).validate(Mockito.eq(IP));
    validateSuccess("/test/strict", VALID_X_FORWARDED_FOR);
    Mockito.doThrow(new RateLimitExceededException(RETRY_AFTER)).when(RATE_LIMITER).validate(Mockito.eq(IP));
    validateFailure("/test/strict", VALID_X_FORWARDED_FOR, RETRY_AFTER);
    Mockito.doNothing().when(RATE_LIMITER).validate(Mockito.eq(IP));
    validateSuccess("/test/strict", VALID_X_FORWARDED_FOR);
    Mockito.doThrow(new RateLimitExceededException(RETRY_AFTER)).when(RATE_LIMITER).validate(Mockito.eq(IP));
    validateFailure("/test/strict", VALID_X_FORWARDED_FOR, RETRY_AFTER);
  }

  @Test
  public void testInvalidHeader() throws Exception {
    Mockito.doNothing().when(RATE_LIMITER).validate(Mockito.eq(IP));
    validateSuccess("/test/strict", VALID_X_FORWARDED_FOR);
    validateFailure("/test/strict", INVALID_X_FORWARDED_FOR, RETRY_AFTER_INVALID_HEADER);
    validateFailure("/test/strict", "", RETRY_AFTER_INVALID_HEADER);

    validateSuccess("/test/loose", VALID_X_FORWARDED_FOR);
    validateSuccess("/test/loose", INVALID_X_FORWARDED_FOR);
    validateSuccess("/test/loose", "");

    // also checking that even if rate limiter is failing -- it doesn't matter in the case of invalid IP
    Mockito.doThrow(new RateLimitExceededException(RETRY_AFTER)).when(RATE_LIMITER).validate(Mockito.anyString());
    validateFailure("/test/loose", VALID_X_FORWARDED_FOR, RETRY_AFTER);
    validateSuccess("/test/loose", INVALID_X_FORWARDED_FOR);
    validateSuccess("/test/loose", "");
  }

  private static void validateSuccess(final String path, final String xff) {
    final Response response = RESOURCES.getJerseyTest()
        .target(path)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, xff)
        .get();

    assertEquals(200, response.getStatus());
  }

  private static void validateFailure(final String path, final String xff, final Duration expectedRetryAfter) {
    final Response response = RESOURCES.getJerseyTest()
        .target(path)
        .request()
        .header(HttpHeaders.X_FORWARDED_FOR, xff)
        .get();

    assertEquals(413, response.getStatus());
    assertEquals("" + expectedRetryAfter.getSeconds(), response.getHeaderString(HttpHeaders.RETRY_AFTER));
  }
}
