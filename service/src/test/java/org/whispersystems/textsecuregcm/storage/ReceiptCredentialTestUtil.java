package org.whispersystems.textsecuregcm.storage;

import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ClientZkReceiptOperations;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequestContext;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class ReceiptCredentialTestUtil {
  private static final ServerSecretParams RECEIPT_PARAMS = ServerSecretParams.generate();

  public static ReceiptCredentialPresentation receiptPresentation()
      throws InvalidInputException, VerificationFailedException {

    return receiptPresentation(new ReceiptSerial(TestRandomUtil.nextBytes(ReceiptSerial.SIZE)), Instant.now().plus(
        Duration.ofDays(30)), 1);
  }

  public static ReceiptCredentialPresentation receiptPresentation(final Instant expiresAt, final long receiptLevel)
      throws InvalidInputException, VerificationFailedException {

    return receiptPresentation(new ReceiptSerial(TestRandomUtil.nextBytes(ReceiptSerial.SIZE)), expiresAt, receiptLevel);
  }

  public static ReceiptCredentialPresentation receiptPresentation(final ReceiptSerial receiptSerial, final Instant expiresAt, final long receiptLevel)
      throws VerificationFailedException {

    final ServerZkReceiptOperations serverOperations = new ServerZkReceiptOperations(RECEIPT_PARAMS);
    final ClientZkReceiptOperations clientOperations =
        new ClientZkReceiptOperations(RECEIPT_PARAMS.getPublicParams());

    final ReceiptCredentialRequestContext requestContext =
        clientOperations.createReceiptCredentialRequestContext(receiptSerial);

    final ReceiptCredentialResponse response = serverOperations.issueReceiptCredential(requestContext.getRequest(),
        expiresAt.truncatedTo(ChronoUnit.DAYS).getEpochSecond(),
        receiptLevel);

    return clientOperations.createReceiptCredentialPresentation(
        clientOperations.receiveReceiptCredential(requestContext, response));
  }
}
