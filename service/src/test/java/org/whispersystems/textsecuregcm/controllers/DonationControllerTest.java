/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.donation.DonationPermit;
import org.signal.libsignal.zkgroup.donation.DonationPermitRequestContext;
import org.signal.libsignal.zkgroup.donation.DonationPermitResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.configuration.BadgeConfiguration;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.entities.BadgeSvg;
import org.whispersystems.textsecuregcm.entities.CreateDonationPermitResponse;
import org.whispersystems.textsecuregcm.entities.CreateDonationPermitsRequest;
import org.whispersystems.textsecuregcm.entities.RedeemReceiptRequest;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DonationPermits;
import org.whispersystems.textsecuregcm.storage.DonationPermitsManager;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.subscriptions.ReceiptCredentialPresentationFactory;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class DonationControllerTest {

  private static final long nowEpochSeconds = 1_500_000_000L;

  static BadgesConfiguration getBadgesConfiguration() {
    return new BadgesConfiguration(
        List.of(
            new BadgeConfiguration("TEST", "other", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG",
                List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))),
            new BadgeConfiguration("TEST1", "testing", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG",
                List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))),
            new BadgeConfiguration("TEST2", "testing", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG",
                List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld"))),
            new BadgeConfiguration("TEST3", "testing", List.of("l", "m", "h", "x", "xx", "xxx"), "SVG",
                List.of(new BadgeSvg("sl", "sd"), new BadgeSvg("ml", "md"), new BadgeSvg("ll", "ld")))),
        List.of("TEST"),
        Map.of(1L, "TEST1", 2L, "TEST2", 3L, "TEST3"));
  }

  private final TestClock clock = TestClock.pinned(Instant.ofEpochSecond(nowEpochSeconds));

  final ServerSecretParams serverSecretParams = ServerSecretParams.generate();
  private final DonationPermitsManager donationPermitsManager = new DonationPermitsManager(mock(DonationPermits.class), serverSecretParams, clock);

  private RedeemedReceiptsManager redeemedReceiptsManager;
  private AccountsManager accountsManager;
  private ReceiptSerial receiptSerial;
  private byte[] presentation;
  private ReceiptCredentialPresentationFactory receiptCredentialPresentationFactory;
  private ReceiptCredentialPresentation receiptCredentialPresentation;
  private RateLimiter createDonationPermitLimiter;
  private ResourceExtension resources;

  @BeforeEach
  void beforeEach() throws Throwable {
    redeemedReceiptsManager = mock(RedeemedReceiptsManager.class);
    accountsManager = mock(AccountsManager.class);
    AccountsHelper.setupMockUpdate(accountsManager);
    receiptSerial = new ReceiptSerial(TestRandomUtil.nextBytes(ReceiptSerial.SIZE));
    presentation = TestRandomUtil.nextBytes(25);
    receiptCredentialPresentationFactory = mock(ReceiptCredentialPresentationFactory.class);
    receiptCredentialPresentation = mock(ReceiptCredentialPresentation.class);

    final RateLimiters rateLimiters = mock(RateLimiters.class);

    createDonationPermitLimiter = mock(RateLimiter.class);
    when(rateLimiters.getCreateDonationPermitLimiter())
        .thenReturn(createDonationPermitLimiter);

    try {
      when(receiptCredentialPresentationFactory.build(presentation)).thenReturn(receiptCredentialPresentation);
    } catch (InvalidInputException e) {
      throw new AssertionError(e);
    }

    resources = ResourceExtension.builder()
        .addProvider(AuthHelper.getAuthFilter())
        .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
        .addProvider(RateLimitExceededExceptionMapper.class)
        .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
        .addResource(new DonationController(clock, mock(ServerZkReceiptOperations.class), redeemedReceiptsManager, accountsManager,
            getBadgesConfiguration(), receiptCredentialPresentationFactory, donationPermitsManager, rateLimiters))
        .build();
    resources.before();
  }

  @AfterEach
  void afterEach() throws Throwable {
    resources.after();
  }

  @Test
  void testRedeemReceipt() {
    when(receiptCredentialPresentation.getReceiptSerial()).thenReturn(receiptSerial);
    final long receiptLevel = 1L;
    when(receiptCredentialPresentation.getReceiptLevel()).thenReturn(receiptLevel);
    final long receiptExpiration = nowEpochSeconds + 86400 * 30;
    when(receiptCredentialPresentation.getReceiptExpirationTime()).thenReturn(receiptExpiration);
    when(redeemedReceiptsManager.put(same(receiptSerial), eq(receiptExpiration), eq(receiptLevel), eq(AuthHelper.VALID_UUID))).thenReturn(true);
    when(accountsManager.getByAccountIdentifier(eq(AuthHelper.VALID_UUID)))
        .thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));

    RedeemReceiptRequest request = new RedeemReceiptRequest(presentation, true, true);
    try (Response response = resources.getJerseyTest()
        .target("/v1/donation/redeem-receipt")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);
    }

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
    when(redeemedReceiptsManager.put(same(receiptSerial), eq(receiptExpiration), eq(receiptLevel), eq(AuthHelper.VALID_UUID))).thenReturn(false);
    when(accountsManager.getByAccountIdentifier(eq(AuthHelper.VALID_UUID)))
        .thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));

    RedeemReceiptRequest request = new RedeemReceiptRequest(presentation, true, true);
    try (Response response = resources.getJerseyTest()
        .target("/v1/donation/redeem-receipt")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(400);
      assertThat(response.readEntity(String.class)).isEqualTo("receipt serial is already redeemed");
    }
  }

  @Test
  void testRedeemReceiptBadCredentialPresentation() throws InvalidInputException {
    when(receiptCredentialPresentationFactory.build(any())).thenThrow(new InvalidInputException());

    try(Response response = resources.getJerseyTest()
        .target("/v1/donation/redeem-receipt")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.entity(new RedeemReceiptRequest(presentation, true, true), MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(400);
    }
  }

  @Test
  void testCreatePermits() {

    final int permitCount = 10;
    final DonationPermitRequestContext context = DonationPermitRequestContext.forCount(permitCount);
    final CreateDonationPermitsRequest request = new CreateDonationPermitsRequest(context.request().serialize());

    try (Response response = resources.getJerseyTest()
        .target("/v1/donation/permit")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(200);

      final DonationPermitResponse donationPermitResponse = assertDoesNotThrow(() -> new DonationPermitResponse(
          response.readEntity(CreateDonationPermitResponse.class).permitResponse()));

      final List<DonationPermit> permits = assertDoesNotThrow(() -> context.receive(donationPermitResponse, serverSecretParams.getPublicParams(), clock.instant()));

      assertThat(permits.size()).isEqualTo(permitCount);
    }
  }

  @Test
  void testCreatePermitsRateLimited() throws Exception {

    final int permitCount = 10;
    final DonationPermitRequestContext context = DonationPermitRequestContext.forCount(permitCount);
    final CreateDonationPermitsRequest request = new CreateDonationPermitsRequest(context.request().serialize());

    final Duration retryDuration = Duration.ofHours(1);
    doThrow(new RateLimitExceededException(retryDuration))
        .when(createDonationPermitLimiter).validate(any(UUID.class), anyLong());

    try (Response response = resources.getJerseyTest()
        .target("/v1/donation/permit")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))) {

      assertThat(response.getStatus()).isEqualTo(429);
      assertThat(response.getHeaderString("Retry-After")).asInt().isEqualTo(retryDuration.toSeconds());
    }
  }

}
