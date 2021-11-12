/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(DropwizardExtensionsSupport.class)
class AcceptNumericOnlineFlagRequestFilterTest {

  @Path("/test")
  public static class TestResource {

    @Path("/convert-boolean")
    @GET
    public boolean convertBoolean(@QueryParam("online") final boolean online) {
      return online;
    }

    @Path("/no-conversion")
    @GET
    public boolean noConversion(@QueryParam("online") final boolean online) {
      return online;
    }
  }

  private static final ResourceExtension RESOURCE_EXTENSION = ResourceExtension.builder()
      .addProvider(new AcceptNumericOnlineFlagRequestFilter("test/convert-boolean"))
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new TestResource())
      .build();

  @ParameterizedTest
  @CsvSource({
      "/test/convert-boolean, true,  true",
      "/test/convert-boolean, false, false",
      "/test/convert-boolean, 1,     true",
      "/test/convert-boolean, 0,     false",
      "/test/no-conversion,   true,  true",
      "/test/no-conversion,   false, false",
      "/test/no-conversion,   1,     false",
      "/test/no-conversion,   0,     false"
  })
  void filter(final String path, final String value, final boolean expected) {
    final boolean response =
        RESOURCE_EXTENSION.getJerseyTest()
            .target(path)
            .queryParam("online", value)
            .request()
            .get(boolean.class);

    assertEquals(expected, response);
  }
}
