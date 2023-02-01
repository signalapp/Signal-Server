package org.whispersystems.textsecuregcm.mappers;

import com.fasterxml.jackson.databind.JsonMappingException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class JsonMappingExceptionMapper implements ExceptionMapper<JsonMappingException> {
  @Override
  public Response toResponse(final JsonMappingException exception) {
    return Response.status(422).build();
  }
}
