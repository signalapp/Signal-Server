package org.whispersystems.textsecuregcm.grpc;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.mockito.Mock;
import org.signal.chat.donations.DonationsGrpc;
import org.signal.chat.donations.RedeemReceiptRequest;
import org.signal.chat.donations.RedeemReceiptResponse;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
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

  private final Clock clock = TestClock.pinned(Instant.ofEpochSecond(100));

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
    return new DonationsGrpcService(
        clock,
        zkReceiptOperations,
        redeemedReceiptsManager,
        accountsManager,
        badgesConfiguration,
        receiptCredentialPresentationFactory
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
}
