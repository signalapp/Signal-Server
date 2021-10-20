/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.whispersystems.textsecuregcm.util.NonNormalizedPhoneNumberException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class NonNormalizedPhoneNumberExceptionMapper implements ExceptionMapper<NonNormalizedPhoneNumberException> {

  private static final Counter NON_NORMALIZED_NUMBER_COUNTER =
      Metrics.counter(name(NonNormalizedPhoneNumberExceptionMapper.class, "nonNormalizedNumbers"));

  @Override
  public Response toResponse(final NonNormalizedPhoneNumberException exception) {
    NON_NORMALIZED_NUMBER_COUNTER.increment();

    return Response.status(Status.BAD_REQUEST)
        .entity(new NonNormalizedPhoneNumberResponse(exception.getOriginalNumber(), exception.getNormalizedNumber()))
        .build();
  }
}
