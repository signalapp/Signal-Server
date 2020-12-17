/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntityList;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.currency.CurrencyConversionManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.dropwizard.auth.Auth;

@Path("/v1/payments")
public class PaymentsController {

  private final ExternalServiceCredentialGenerator paymentsServiceCredentialGenerator;
  private final CurrencyConversionManager          currencyManager;

  public PaymentsController(CurrencyConversionManager currencyManager, ExternalServiceCredentialGenerator paymentsServiceCredentialGenerator) {
    this.currencyManager                    = currencyManager;
    this.paymentsServiceCredentialGenerator = paymentsServiceCredentialGenerator;
  }

  @Timed
  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  public ExternalServiceCredentials getAuth(@Auth Account account) {
    return paymentsServiceCredentialGenerator.generateFor(account.getUuid().toString());
  }

  @Timed
  @GET
  @Path("/conversions")
  @Produces(MediaType.APPLICATION_JSON)
  public CurrencyConversionEntityList getConversions(@Auth Account account) {
    return currencyManager.getCurrencyConversions().orElseThrow();
  }
}
