package org.whispersystems.textsecuregcm.purchases;

import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;

public interface ReceiptCredentialPresentationFactory {

  ReceiptCredentialPresentation build(byte[] bytes) throws InvalidInputException;
}
