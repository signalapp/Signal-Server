package org.whispersystems.textsecuregcm.mappers;

import com.fasterxml.jackson.databind.JsonMappingException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;

public class JsonMappingExceptionMapper implements ExceptionMapper<JsonMappingException> {
  @Override
  public Response toResponse(final JsonMappingException exception) {
    if (exception.getCause() instanceof java.util.concurrent.TimeoutException) {
      return Response.status(Response.Status.REQUEST_TIMEOUT).build();
    }
    if (exception.getCause() instanceof org.eclipse.jetty.io.EofException
        || exception.getMessage() != null && exception.getMessage().startsWith("Early EOF")) {
      // Some sort of timeout or broken connection
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    return Response.status(422).build();
  }
}
