/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import io.micrometer.core.instrument.Metrics;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.ext.ExceptionMapper;
import org.whispersystems.textsecuregcm.util.NonNormalizedPhoneNumberException;

public class NonNormalizedPhoneNumberExceptionMapper implements ExceptionMapper<NonNormalizedPhoneNumberException> {

  private static final String NON_NORMALIZED_NUMBER_COUNTER_NAME =
      name(NonNormalizedPhoneNumberExceptionMapper.class, "nonNormalizedNumbers");

  @Override
  public Response toResponse(final NonNormalizedPhoneNumberException exception) {
    String countryCode;

    try {
      countryCode =
          String.valueOf(PhoneNumberUtil.getInstance().parse(exception.getOriginalNumber(), null).getCountryCode());
    } catch (final NumberParseException ignored) {
      countryCode = "unknown";
    }

    Metrics.counter(NON_NORMALIZED_NUMBER_COUNTER_NAME, "countryCode", countryCode).increment();

    return Response.status(Status.BAD_REQUEST)
        .entity(new NonNormalizedPhoneNumberResponse(exception.getOriginalNumber(), exception.getNormalizedNumber()))
        .build();
  }
}
