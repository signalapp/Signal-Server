package org.whispersystems.textsecuregcm.grpc;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.mockito.Mock;
import org.signal.chat.donations.CreateDonationPermitRequest;
import org.signal.chat.donations.CreateDonationPermitResponse;
import org.signal.chat.donations.DonationsGrpc;
import org.signal.chat.donations.RedeemReceiptRequest;
import org.signal.chat.donations.RedeemReceiptResponse;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.donation.DonationPermit;
import org.signal.libsignal.zkgroup.donation.DonationPermitRequestContext;
import org.signal.libsignal.zkgroup.donation.DonationPermitResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DonationPermits;
import org.whispersystems.textsecuregcm.storage.DonationPermitsManager;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.subscriptions.ReceiptCredentialPresentationFactory;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class DonationsGrpcServiceTest extends SimpleBaseGrpcTest<DonationsGrpcService, DonationsGrpc.DonationsBlockingStub> {

  @Mock
  private ServerZkReceiptOperations zkReceiptOperations;

  @Mock
  private RedeemedReceiptsManager redeemedReceiptsManager;

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private Account account;

  @Mock
  private ReceiptCredentialPresentationFactory receiptCredentialPresentationFactory;

  @Mock
  private BadgesConfiguration badgesConfiguration;

  @Mock
  private ReceiptSerial receiptSerial;

  private final ServerSecretParams donationsPermitSecretParams = ServerSecretParams.generate();

  private RateLimiter createDonationPermitLimiter;

  private DonationPermitsManager donationPermitsManager;

  private final TestClock clock = TestClock.pinned(Instant.ofEpochSecond(100));

  private static final long EXPIRATION_TIME_EPOCH_SECONDS = 200;

  @Override
  protected DonationsGrpcService createServiceBeforeEachTest() {
    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI)).thenReturn(Optional.of(account));
    AccountsHelper.setupMockUpdate(accountsManager);
    final ReceiptCredentialPresentation receiptCredentialPresentation = mock(ReceiptCredentialPresentation.class);
    try {
      when(receiptCredentialPresentationFactory.build(any())).thenReturn(receiptCredentialPresentation);
    } catch (final InvalidInputException e) {
      throw new RuntimeException(e);
    }
    when(receiptCredentialPresentation.getReceiptLevel()).thenReturn(1L);
    when(badgesConfiguration.getReceiptLevels()).thenReturn(Map.of(1L, "testBadge"));
    when(receiptCredentialPresentation.getReceiptExpirationTime()).thenReturn(EXPIRATION_TIME_EPOCH_SECONDS);
    when(receiptCredentialPresentation.getReceiptSerial()).thenReturn(receiptSerial);

    donationPermitsManager = new DonationPermitsManager(mock(DonationPermits.class), donationsPermitSecretParams,
        clock);

    final RateLimiters rateLimiters = mock(RateLimiters.class);
    createDonationPermitLimiter = mock(RateLimiter.class);
    when(rateLimiters.getCreateDonationPermitLimiter()).thenReturn(createDonationPermitLimiter);
    return new DonationsGrpcService(
        clock,
        zkReceiptOperations,
        redeemedReceiptsManager,
        accountsManager,
        badgesConfiguration,
        receiptCredentialPresentationFactory,
        donationPermitsManager,
        rateLimiters
    );
  }

  @CartesianTest
  void redeemReceipt(
      @CartesianTest.Values(booleans = {true, false}) final boolean isVisible,
      @CartesianTest.Values(booleans = {true, false}) final boolean isPrimary) {
    when(redeemedReceiptsManager.put(receiptSerial, EXPIRATION_TIME_EPOCH_SECONDS, 1, AUTHENTICATED_ACI)).thenReturn(
        true);

    final RedeemReceiptResponse response = authenticatedServiceStub().redeemReceipt(RedeemReceiptRequest.newBuilder()
        .setVisible(isVisible)
        .setPrimary(isPrimary)
        .setReceiptCredentialPresentation(ByteString.copyFrom(TestRandomUtil.nextBytes(329)))
        .build());

    assertEquals(RedeemReceiptResponse.ResponseCase.SUCCESS, response.getResponseCase());

    verify(account).addBadge(clock,
        new AccountBadge("testBadge", Instant.ofEpochSecond(EXPIRATION_TIME_EPOCH_SECONDS), isVisible));
    if (isPrimary) {
      verify(account).makeBadgePrimaryIfExists(clock, "testBadge");
    } else {
      verify(account, never()).makeBadgePrimaryIfExists(any(), any());
    }
  }

  @Test
  void alreadyRedeemed() {
    when(redeemedReceiptsManager.put(receiptSerial, EXPIRATION_TIME_EPOCH_SECONDS, 1, AUTHENTICATED_ACI)).thenReturn(
        false);

    final RedeemReceiptResponse response = authenticatedServiceStub().redeemReceipt(RedeemReceiptRequest.newBuilder()
        .setVisible(true)
        .setPrimary(true)
        .setReceiptCredentialPresentation(ByteString.copyFrom(TestRandomUtil.nextBytes(329)))
        .build());

    assertEquals(RedeemReceiptResponse.ResponseCase.ALREADY_REDEEMED, response.getResponseCase());
    verifyNoInteractions(account);
  }

  @ParameterizedTest
  @CsvSource({"true,false", "false, true"})
  void failedZkAuthentication(final boolean invalidPresentation, final boolean verificationFailed)
      throws InvalidInputException, VerificationFailedException {
    if (invalidPresentation) {
      doThrow(InvalidInputException.class).when(receiptCredentialPresentationFactory).build(any());
    }
    if (verificationFailed) {
      doThrow(VerificationFailedException.class).when(zkReceiptOperations).verifyReceiptCredentialPresentation(any());
    }
    final RedeemReceiptResponse response = authenticatedServiceStub().redeemReceipt(RedeemReceiptRequest.newBuilder()
        .setVisible(true)
        .setPrimary(true)
        .setReceiptCredentialPresentation(ByteString.copyFrom(TestRandomUtil.nextBytes(329)))
        .build());
    assertEquals(RedeemReceiptResponse.ResponseCase.FAILED_AUTHENTICATION, response.getResponseCase());
    verifyNoInteractions(account);
  }

  @Test
  void createDonationPermit() throws Exception {
    final int permitCount = 10;
    final DonationPermitRequestContext context = DonationPermitRequestContext.forCount(permitCount);

    final CreateDonationPermitResponse response = authenticatedServiceStub().createDonationPermit(
        CreateDonationPermitRequest.newBuilder()
            .setDonationPermitRequest(ByteString.copyFrom(context.request().serialize()))
            .build());

    final DonationPermitResponse donationPermitResponse = new DonationPermitResponse(response.getDonationPermitResponse().toByteArray());
    final List<DonationPermit> donationPermits = context.receive(donationPermitResponse,
        donationsPermitSecretParams.getPublicParams(),
        clock.instant());

    assertEquals(permitCount, donationPermits.size());
  }

  @Test
  void createDonationPermitInvalid() throws Exception {
    final int permitCount = 10;
    final DonationPermitRequestContext context = DonationPermitRequestContext.forCount(permitCount);

    GrpcTestUtils.assertStatusInvalidArgument(() ->
        authenticatedServiceStub().createDonationPermit(CreateDonationPermitRequest.newBuilder()
            .setDonationPermitRequest(ByteString.copyFrom(new byte[]{1, 2, 3}))
            .build()));
  }

  @Test
  void createDonationPermitRateLimited() throws Exception{
    final Duration retryDuration = Duration.ofHours(3);
    doThrow(new RateLimitExceededException(retryDuration))
        .when(createDonationPermitLimiter).validate(any(UUID.class), anyLong());

    final int permitCount = 10;
    final DonationPermitRequestContext context = DonationPermitRequestContext.forCount(permitCount);

    GrpcTestUtils.assertRateLimitExceeded(retryDuration, () ->
        authenticatedServiceStub().createDonationPermit(CreateDonationPermitRequest.newBuilder()
            .setDonationPermitRequest(ByteString.copyFrom(context.request().serialize()))
            .build()));

  }

}
