package org.whispersystems.textsecuregcm.mappers;

import io.micrometer.core.instrument.Metrics;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.ObsoletePhoneNumberFormatException;

public class ObsoletePhoneNumberFormatExceptionMapper implements ExceptionMapper<ObsoletePhoneNumberFormatException> {

  private static final String COUNTER_NAME = MetricsUtil.name(ObsoletePhoneNumberFormatExceptionMapper.class, "errors");

  @Override
  public Response toResponse(final ObsoletePhoneNumberFormatException exception) {
    Metrics.counter(COUNTER_NAME, "regionCode", exception.getRegionCode()).increment();
    return Response.status(499).build();
  }
}
