/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.core.Response;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.currency.CurrencyConversionManager;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntity;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntityList;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

@ExtendWith(DropwizardExtensionsSupport.class)
class PaymentsControllerTest {

  private static final ExternalServiceCredentialsGenerator paymentsCredentialsGenerator = mock(ExternalServiceCredentialsGenerator.class);
  private static final CurrencyConversionManager currencyManager                      = mock(CurrencyConversionManager.class);

  private final ExternalServiceCredentials validCredentials = new ExternalServiceCredentials("username", "password");

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new PaymentsController(currencyManager, paymentsCredentialsGenerator))
      .build();


  @BeforeEach
  void setup() {
    when(paymentsCredentialsGenerator.generateForUuid(eq(AuthHelper.VALID_UUID))).thenReturn(validCredentials);
    when(currencyManager.getCurrencyConversions()).thenReturn(Optional.of(
        new CurrencyConversionEntityList(List.of(
            new CurrencyConversionEntity("FOO", Map.of(
                "USD", new BigDecimal("2.35"),
                "EUR", new BigDecimal("1.89")
            )),
            new CurrencyConversionEntity("BAR", Map.of(
                "USD", new BigDecimal("1.50"),
                "EUR", new BigDecimal("0.98")
            ))
        ), System.currentTimeMillis())));
  }

  @Test
  void testGetAuthToken() {
    ExternalServiceCredentials token =
        resources.getJerseyTest()
            .target("/v1/payments/auth")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .get(ExternalServiceCredentials.class);

    assertThat(token.username()).isEqualTo(validCredentials.username());
    assertThat(token.password()).isEqualTo(validCredentials.password());
  }

  @Test
  void testInvalidAuthGetAuthToken() {
    Response response =
        resources.getJerseyTest()
            .target("/v1/payments/auth")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.INVALID_UUID, AuthHelper.INVALID_PASSWORD))
            .get();

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testGetCurrencyConversions() {
    CurrencyConversionEntityList conversions =
        resources.getJerseyTest()
                 .target("/v1/payments/conversions")
                 .request()
                 .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .get(CurrencyConversionEntityList.class);


    assertThat(conversions.getCurrencies().size()).isEqualTo(2);
    assertThat(conversions.getCurrencies().get(0).getBase()).isEqualTo("FOO");
    assertThat(conversions.getCurrencies().get(0).getConversions().get("USD")).isEqualTo(new BigDecimal("2.35"));
  }

  @Test
  void testGetCurrencyConversions_Json() {
    String json =
        resources.getJerseyTest()
            .target("/v1/payments/conversions")
            .request()
            .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .get(String.class);

    // the currency serialization might occur in either order
    assertThat(json).containsPattern("\\{(\"EUR\":1.89,\"USD\":2.35|\"USD\":2.35,\"EUR\":1.89)}");
  }

}
