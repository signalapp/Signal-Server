/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.mappers;

import java.util.Map;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessorException;

public class SubscriptionProcessorExceptionMapper implements ExceptionMapper<SubscriptionProcessorException> {

  public static final int EXTERNAL_SERVICE_ERROR_STATUS_CODE = 440;

  @Override
  public Response toResponse(final SubscriptionProcessorException exception) {
    return Response.status(EXTERNAL_SERVICE_ERROR_STATUS_CODE)
        .entity(Map.of(
            "processor", exception.getProcessor().name(),
            "chargeFailure", exception.getChargeFailure()
        ))
        .build();
  }
}
