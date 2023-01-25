/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.commons.codec.DecoderException;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.PaymentsServiceConfiguration;
import org.whispersystems.textsecuregcm.currency.CurrencyConversionManager;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntityList;

@Path("/v1/payments")
public class PaymentsController {

  private final ExternalServiceCredentialsGenerator paymentsServiceCredentialsGenerator;
  private final CurrencyConversionManager currencyManager;

  
  public static ExternalServiceCredentialsGenerator credentialsGenerator(final PaymentsServiceConfiguration cfg)
      throws DecoderException {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.getUserAuthenticationTokenSharedSecret())
        .prependUsername(true)
        .build();
  }

  public PaymentsController(final CurrencyConversionManager currencyManager, final ExternalServiceCredentialsGenerator paymentsServiceCredentialsGenerator) {
    this.currencyManager                    = currencyManager;
    this.paymentsServiceCredentialsGenerator = paymentsServiceCredentialsGenerator;
  }

  @Timed
  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  public ExternalServiceCredentials getAuth(final @Auth AuthenticatedAccount auth) {
    return paymentsServiceCredentialsGenerator.generateForUuid(auth.getAccount().getUuid());
  }

  @Timed
  @GET
  @Path("/conversions")
  @Produces(MediaType.APPLICATION_JSON)
  public CurrencyConversionEntityList getConversions(final @Auth AuthenticatedAccount auth) {
    return currencyManager.getCurrencyConversions().orElseThrow();
  }
}
