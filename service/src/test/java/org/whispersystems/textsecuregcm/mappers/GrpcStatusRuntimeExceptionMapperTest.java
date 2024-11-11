/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dropwizard.jersey.errors.ErrorMessage;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.grpc.Status;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.stream.Stream;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class GrpcStatusRuntimeExceptionMapperTest {

  private static final GrpcStatusRuntimeExceptionMapper exceptionMapper = new GrpcStatusRuntimeExceptionMapper();
  private static final TestController testController = new TestController();

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(new CompletionExceptionMapper())
      .addProvider(exceptionMapper)
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(testController)
      .build();

  @BeforeEach
  public void setUp() {
    testController.exception = null;
  }

  @ParameterizedTest
  @ValueSource(strings = {"json", "text"})
  public void responseBody(final String path) throws JsonProcessingException {
    testController.exception = Status.INVALID_ARGUMENT.withDescription("oofta").asRuntimeException();
    final Response response = resources.getJerseyTest().target("/v1/test/" + path).request().get();
    assertThat(response.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    final ErrorMessage body = SystemMapper.jsonMapper().readValue(
        response.readEntity(String.class),
        ErrorMessage.class);

    assertThat(body.getMessage()).isEqualTo(testController.exception.getMessage());
    assertThat(body.getCode()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
  }

  public static Stream<Arguments> errorMapping() {
    return Stream.of(
        Arguments.of(Status.INVALID_ARGUMENT, 400),
        Arguments.of(Status.NOT_FOUND, 404),
        Arguments.of(Status.UNAVAILABLE, 500));
  }

  @ParameterizedTest
  @MethodSource
  public void errorMapping(final Status status, final int expectedHttpCode) {
    testController.exception = status.asRuntimeException();
    final Response response = resources.getJerseyTest().target("/v1/test/json").request().get();
    assertThat(response.getStatus()).isEqualTo(expectedHttpCode);
  }

  @Path("/v1/test")
  public static class TestController {

    volatile RuntimeException exception = null;

    @GET
    @Path("/text")
    public Response plaintext() {
      if (exception != null) {
        throw exception;
      }
      return Response.ok().build();
    }

    @GET
    @Path("/json")
    @Produces(MediaType.APPLICATION_JSON)
    public Response json() {
      if (exception != null) {
        throw exception;
      }
      return Response.ok().build();
    }
  }
}
