/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.controllers;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.DonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.controllers.DonationController;
import org.whispersystems.textsecuregcm.entities.ApplePayAuthorizationRequest;
import org.whispersystems.textsecuregcm.entities.ApplePayAuthorizationResponse;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public class DonationControllerTest {

  private static final Executor executor = Executors.newSingleThreadExecutor();

  @Rule
  public final WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort().dynamicHttpsPort());

  private ResourceExtension resources;

  @Before
  public void before() throws Throwable {
    DonationConfiguration configuration = new DonationConfiguration();
    configuration.setApiKey("test-api-key");
    configuration.setUri("http://localhost:" + wireMockRule.port() + "/foo/bar");
    configuration.setCircuitBreaker(new CircuitBreakerConfiguration());
    configuration.setRetry(new RetryConfiguration());
    configuration.setSupportedCurrencies(Set.of("usd", "gbp"));
    resources = ResourceExtension.builder()
        .addProvider(AuthHelper.getAuthFilter())
        .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(ImmutableSet.of(Account.class, DisabledPermittedAccount.class)))
        .setMapper(SystemMapper.getMapper())
        .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
        .addResource(new DonationController(executor, configuration))
        .build();
    resources.before();
  }

  @After
  public void after() throws Throwable {
    resources.after();
  }

  @Test
  public void testGetApplePayAuthorizationReturns200() {
    wireMockRule.stubFor(post(urlEqualTo("/foo/bar"))
        .withBasicAuth("test-api-key", "")
        .willReturn(aResponse()
            .withHeader("Content-Type", MediaType.APPLICATION_JSON)
            .withBody("{\"id\":\"an_id\",\"client_secret\":\"some_secret\"}")));

    ApplePayAuthorizationRequest request = new ApplePayAuthorizationRequest();
    request.setCurrency("usd");
    request.setAmount(1000);
    Response response = resources.getJerseyTest()
        .target("/v1/donation/authorize-apple-pay")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);

    ApplePayAuthorizationResponse responseObject = response.readEntity(ApplePayAuthorizationResponse.class);
    assertThat(responseObject.getId()).isEqualTo("an_id");
    assertThat(responseObject.getClientSecret()).isEqualTo("some_secret");
  }

  @Test
  public void testGetApplePayAuthorizationWithoutAuthHeaderReturns401() {
    ApplePayAuthorizationRequest request = new ApplePayAuthorizationRequest();
    request.setCurrency("usd");
    request.setAmount(1000);
    Response response = resources.getJerseyTest()
        .target("/v1/donation/authorize-apple-pay")
        .request()
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  public void testGetApplePayAuthorizationWithUnsupportedCurrencyReturns422() {
    ApplePayAuthorizationRequest request = new ApplePayAuthorizationRequest();
    request.setCurrency("zzz");
    request.setAmount(1000);
    Response response = resources.getJerseyTest()
        .target("/v1/donation/authorize-apple-pay")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_NUMBER, AuthHelper.VALID_PASSWORD))
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(422);
  }
}
