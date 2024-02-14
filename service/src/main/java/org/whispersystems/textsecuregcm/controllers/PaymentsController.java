/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.tags.Tag;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.PaymentsServiceConfiguration;
import org.whispersystems.textsecuregcm.currency.CurrencyConversionManager;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntityList;
import org.whispersystems.websocket.auth.ReadOnly;

@Path("/v1/payments")
@Tag(name = "Payments")
public class PaymentsController {

  private final ExternalServiceCredentialsGenerator paymentsServiceCredentialsGenerator;
  private final CurrencyConversionManager currencyManager;


  public static ExternalServiceCredentialsGenerator credentialsGenerator(final PaymentsServiceConfiguration cfg) {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .prependUsername(true)
        .build();
  }

  public PaymentsController(final CurrencyConversionManager currencyManager,
      final ExternalServiceCredentialsGenerator paymentsServiceCredentialsGenerator) {
    this.currencyManager = currencyManager;
    this.paymentsServiceCredentialsGenerator = paymentsServiceCredentialsGenerator;
  }

  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  public ExternalServiceCredentials getAuth(final @ReadOnly @Auth AuthenticatedAccount auth) {
    return paymentsServiceCredentialsGenerator.generateForUuid(auth.getAccount().getUuid());
  }

  @GET
  @Path("/conversions")
  @Produces(MediaType.APPLICATION_JSON)
  public CurrencyConversionEntityList getConversions(final @ReadOnly @Auth AuthenticatedAccount auth) {
    return currencyManager.getCurrencyConversions().orElseThrow();
  }
}
