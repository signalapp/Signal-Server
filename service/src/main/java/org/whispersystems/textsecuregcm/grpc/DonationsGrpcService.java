package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import java.time.Clock;
import java.time.Instant;
import org.signal.chat.donations.CreateDonationPermitRequest;
import org.signal.chat.donations.CreateDonationPermitResponse;
import org.signal.chat.donations.RedeemReceiptRequest;
import org.signal.chat.donations.RedeemReceiptResponse;
import org.signal.chat.donations.SimpleDonationsGrpc;
import org.signal.chat.errors.FailedPrecondition;
import org.signal.chat.errors.FailedZkAuthentication;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.donation.DonationPermitRequest;
import org.signal.libsignal.zkgroup.donation.DonationPermitResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DonationPermitsManager;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;
import org.whispersystems.textsecuregcm.subscriptions.ReceiptCredentialPresentationFactory;

public class DonationsGrpcService extends SimpleDonationsGrpc.DonationsImplBase {

  private final Clock clock;
  private final ServerZkReceiptOperations serverZkReceiptOperations;
  private final RedeemedReceiptsManager redeemedReceiptsManager;
  private final AccountsManager accountsManager;
  private final BadgesConfiguration badgesConfiguration;
  private final ReceiptCredentialPresentationFactory receiptCredentialPresentationFactory;
  private final DonationPermitsManager donationPermitsManager;
  private final RateLimiters rateLimiters;

  private static final Logger LOGGER = LoggerFactory.getLogger(DonationsGrpcService.class);

  public DonationsGrpcService(
      final Clock clock,
      final ServerZkReceiptOperations serverZkReceiptOperations,
      final RedeemedReceiptsManager redeemedReceiptsManager,
      final AccountsManager accountsManager,
      final BadgesConfiguration badgesConfiguration,
      final ReceiptCredentialPresentationFactory receiptCredentialPresentationFactory,
      final DonationPermitsManager donationPermitsManager,
      final RateLimiters rateLimiters) {

    this.clock = clock;
    this.serverZkReceiptOperations = serverZkReceiptOperations;
    this.redeemedReceiptsManager = redeemedReceiptsManager;
    this.accountsManager = accountsManager;
    this.badgesConfiguration = badgesConfiguration;
    this.receiptCredentialPresentationFactory = receiptCredentialPresentationFactory;
    this.donationPermitsManager = donationPermitsManager;
    this.rateLimiters = rateLimiters;
  }

  @Override
  public RedeemReceiptResponse redeemReceipt(final RedeemReceiptRequest request) {
    try {
      final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
      final ReceiptCredentialPresentation receiptCredentialPresentation = receiptCredentialPresentationFactory
          .build(request.getReceiptCredentialPresentation().toByteArray());
      serverZkReceiptOperations.verifyReceiptCredentialPresentation(receiptCredentialPresentation);
      final ReceiptSerial receiptSerial = receiptCredentialPresentation.getReceiptSerial();
      final Instant receiptExpiration = Instant.ofEpochSecond(receiptCredentialPresentation.getReceiptExpirationTime());
      final long receiptLevel = receiptCredentialPresentation.getReceiptLevel();
      final String badgeId = badgesConfiguration.getReceiptLevels().get(receiptLevel);
      if (badgeId == null) {
        // Since the receipt presentation checked out, the server messed up because it doesn't recognize a receipt level it previously issued.
        LOGGER.error("Server doesn't recognize previously issued receipt level; please check badgesConfiguration for issues");
        throw GrpcExceptions.unavailable("server does not recognize the requested receipt level");
      }
      final boolean receiptMatched = redeemedReceiptsManager.put(
          receiptSerial, receiptExpiration.getEpochSecond(), receiptLevel, authenticatedDevice.accountIdentifier());
      if (!receiptMatched) {
        return RedeemReceiptResponse.newBuilder()
            .setAlreadyRedeemed(FailedPrecondition.newBuilder()
                .setDescription("receipt has already been redeemed")
                .build())
            .build();
      }

      accountsManager.update(authenticatedDevice.accountIdentifier(), a -> {
        a.addBadge(clock, new AccountBadge(badgeId, receiptExpiration, request.getVisible()));
        if (request.getPrimary()) {
          a.makeBadgePrimaryIfExists(clock, badgeId);
        }
      });

      return RedeemReceiptResponse.newBuilder()
          .setSuccess(Empty.getDefaultInstance())
          .build();

    } catch (final InvalidInputException e) {
      return RedeemReceiptResponse.newBuilder()
          .setFailedAuthentication(FailedZkAuthentication.newBuilder()
              .setDescription("invalid receipt credential presentation")
              .build())
          .build();
    } catch (final VerificationFailedException e) {
      return RedeemReceiptResponse.newBuilder()
          .setFailedAuthentication(FailedZkAuthentication.newBuilder()
              .setDescription("receipt credential presentation verification failed")
              .build())
          .build();
    }
  }

  @Override
  public CreateDonationPermitResponse createDonationPermit(final CreateDonationPermitRequest request) throws Exception {
    final DonationPermitRequest permitRequest;
    try {
      permitRequest = new DonationPermitRequest(request.getDonationPermitRequest().toByteArray());
    } catch (InvalidInputException e) {
      throw GrpcExceptions.invalidArguments("invalid permit request");
    }

    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    rateLimiters.getCreateDonationPermitLimiter().validate(authenticatedDevice.accountIdentifier(), permitRequest.getPermitCount());

    final DonationPermitResponse permitResponse = donationPermitsManager.issue(permitRequest);

    return CreateDonationPermitResponse.newBuilder()
        .setDonationPermitResponse(ByteString.copyFrom(permitResponse.serialize()))
        .build();
  }
}
