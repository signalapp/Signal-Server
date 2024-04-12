package org.whispersystems.textsecuregcm.mappers;

import com.fasterxml.jackson.databind.JsonMappingException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import java.util.concurrent.TimeoutException;

public class JsonMappingExceptionMapper implements ExceptionMapper<JsonMappingException> {
  @Override
  public Response toResponse(final JsonMappingException exception) {
    if (exception.getCause() instanceof TimeoutException) {
      return Response.status(Response.Status.REQUEST_TIMEOUT).build();
    }
    if ("Early EOF".equals(exception.getMessage())) {
      // Some sort of timeout or broken connection
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    return Response.status(422).build();
  }
}
