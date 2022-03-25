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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.configuration.BadgeConfiguration;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.DonationConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.configuration.StripeConfiguration;
import org.whispersystems.textsecuregcm.controllers.DonationController;
import org.whispersystems.textsecuregcm.entities.ApplePayAuthorizationRequest;
import org.whispersystems.textsecuregcm.entities.ApplePayAuthorizationResponse;
import org.whispersystems.textsecuregcm.entities.BadgeSvg;
import org.whispersystems.textsecuregcm.entities.RedeemReceiptRequest;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;

class DonationControllerTest {

  private static final Executor httpClientExecutor = Executors.newSingleThreadExecutor();
  private static final long nowEpochSeconds = 1_500_000_000L;

  @RegisterExtension
  static WireMockExtension wm = WireMockExtension.newInstance()
      .options(wireMockConfig().dynamicPort().dynamicHttpsPort())
      .build();

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  static DonationConfiguration getDonationConfiguration() {
    DonationConfiguration configuration = new DonationConfiguration();
    configuration.setDescription("some description");
    configuration.setUri("http://localhost:" + wm.getRuntimeInfo().getHttpPort() + "/foo/bar");
    configuration.setCircuitBreaker(new CircuitBreakerConfiguration());
    configuration.setRetry(new RetryConfiguration());
    configuration.setSupportedCurrencies(Set.of("usd", "gbp"));
    return configuration;
  }

  static StripeConfiguration getStripeConfiguration() {
    return new StripeConfiguration("test-api-key", new byte[16], "Boost Description String");
  }

  static BadgesConfiguration getBadgesConfiguration() {
    return new BadgesConfiguration(
        List.of(
            new BadgeConfiguration("TEST", "other", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))),
            new BadgeConfiguration("TEST1", "testing", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))),
            new BadgeConfiguration("TEST2", "testing", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))),
            new BadgeConfiguration("TEST3", "testing", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG", List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld")))),
        List.of("TEST"),
        Map.of(1L, "TEST1", 2L, "TEST2", 3L, "TEST3"));
  }

  Clock clock;
  ServerZkReceiptOperations zkReceiptOperations;
  RedeemedReceiptsManager redeemedReceiptsManager;
  AccountsManager accountsManager;
  byte[] receiptSerialBytes;
  ReceiptSerial receiptSerial;
  byte[] presentation;
  DonationController.ReceiptCredentialPresentationFactory receiptCredentialPresentationFactory;
  ReceiptCredentialPresentation receiptCredentialPresentation;
  ResourceExtension resources;

  @BeforeEach
  void beforeEach() throws Throwable {
    clock = mock(Clock.class);
    zkReceiptOperations = mock(ServerZkReceiptOperations.class);
    redeemedReceiptsManager = mock(RedeemedReceiptsManager.class);
    accountsManager = mock(AccountsManager.class);
    AccountsHelper.setupMockUpdate(accountsManager);
    receiptSerialBytes = new byte[ReceiptSerial.SIZE];
    SECURE_RANDOM.nextBytes(receiptSerialBytes);
    receiptSerial = new ReceiptSerial(receiptSerialBytes);
    presentation = new byte[25];
    SECURE_RANDOM.nextBytes(presentation);
    receiptCredentialPresentationFactory = mock(DonationController.ReceiptCredentialPresentationFactory.class);
    receiptCredentialPresentation = mock(ReceiptCredentialPresentation.class);

    when(clock.millis()).thenReturn(nowEpochSeconds * 1000L);
    when(clock.instant()).thenReturn(Instant.ofEpochSecond(nowEpochSeconds));

    try {
      when(receiptCredentialPresentationFactory.build(presentation)).thenReturn(receiptCredentialPresentation);
    } catch (InvalidInputException e) {
      throw new AssertionError(e);
    }

    resources = ResourceExtension.builder()
        .addProvider(AuthHelper.getAuthFilter())
        .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
            ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
        .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
        .addResource(new DonationController(clock, zkReceiptOperations, redeemedReceiptsManager, accountsManager,
            getBadgesConfiguration(), receiptCredentialPresentationFactory, httpClientExecutor,
            getDonationConfiguration(), getStripeConfiguration()))
        .build();
    resources.before();
  }

  @AfterEach
  void afterEach() throws Throwable {
    resources.after();
  }

  @Test
  void testGetApplePayAuthorizationReturns200() {
    wm.stubFor(post(urlEqualTo("/foo/bar"))
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
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);

    ApplePayAuthorizationResponse responseObject = response.readEntity(ApplePayAuthorizationResponse.class);
    assertThat(responseObject.getId()).isEqualTo("an_id");
    assertThat(responseObject.getClientSecret()).isEqualTo("some_secret");
  }

  @Test
  void testGetApplePayAuthorizationWithoutAuthHeaderReturns401() {
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
  void testGetApplePayAuthorizationWithUnsupportedCurrencyReturns422() {
    ApplePayAuthorizationRequest request = new ApplePayAuthorizationRequest();
    request.setCurrency("zzz");
    request.setAmount(1000);
    Response response = resources.getJerseyTest()
        .target("/v1/donation/authorize-apple-pay")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(422);
  }

  @Test
  void testRedeemReceipt() {
    when(receiptCredentialPresentation.getReceiptSerial()).thenReturn(receiptSerial);
    final long receiptLevel = 1L;
    when(receiptCredentialPresentation.getReceiptLevel()).thenReturn(receiptLevel);
    final long receiptExpiration = nowEpochSeconds + 86400 * 30;
    when(receiptCredentialPresentation.getReceiptExpirationTime()).thenReturn(receiptExpiration);
    when(redeemedReceiptsManager.put(same(receiptSerial), eq(receiptExpiration), eq(receiptLevel), eq(AuthHelper.VALID_UUID))).thenReturn(
        CompletableFuture.completedFuture(Boolean.TRUE));
    when(accountsManager.getByAccountIdentifier(eq(AuthHelper.VALID_UUID))).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));

    RedeemReceiptRequest request = new RedeemReceiptRequest(presentation, true, true);
    Response response = resources.getJerseyTest()
        .target("/v1/donation/redeem-receipt")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(200);
    verify(AuthHelper.VALID_ACCOUNT).addBadge(same(clock), eq(new AccountBadge("TEST1", Instant.ofEpochSecond(receiptExpiration), true)));
    verify(AuthHelper.VALID_ACCOUNT).makeBadgePrimaryIfExists(same(clock), eq("TEST1"));
  }

  @Test
  void testRedeemReceiptAlreadyRedeemedWithDifferentParameters() {
    when(receiptCredentialPresentation.getReceiptSerial()).thenReturn(receiptSerial);
    final long receiptLevel = 1L;
    when(receiptCredentialPresentation.getReceiptLevel()).thenReturn(receiptLevel);
    final long receiptExpiration = nowEpochSeconds + 86400 * 30;
    when(receiptCredentialPresentation.getReceiptExpirationTime()).thenReturn(receiptExpiration);
    when(redeemedReceiptsManager.put(same(receiptSerial), eq(receiptExpiration), eq(receiptLevel), eq(AuthHelper.VALID_UUID))).thenReturn(
        CompletableFuture.completedFuture(Boolean.FALSE));

    RedeemReceiptRequest request = new RedeemReceiptRequest(presentation, true, true);
    Response response = resources.getJerseyTest()
        .target("/v1/donation/redeem-receipt")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(400);
    assertThat(response.readEntity(String.class)).isEqualTo("receipt serial is already redeemed");
  }

  @Test
  void testRedeemReceiptBadCredentialPresentation() throws InvalidInputException {
    when(receiptCredentialPresentationFactory.build(any())).thenThrow(new InvalidInputException());
  
    final Response response = resources.getJerseyTest()
        .target("/v1/donation/redeem-receipt")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.entity(new RedeemReceiptRequest(presentation, true, true), MediaType.APPLICATION_JSON_TYPE));
  
    assertThat(response.getStatus()).isEqualTo(400);
  }
}
