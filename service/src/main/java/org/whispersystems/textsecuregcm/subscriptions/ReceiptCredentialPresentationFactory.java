package org.whispersystems.textsecuregcm.subscriptions;

import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;

public interface ReceiptCredentialPresentationFactory {

  ReceiptCredentialPresentation build(byte[] bytes) throws InvalidInputException;
}
