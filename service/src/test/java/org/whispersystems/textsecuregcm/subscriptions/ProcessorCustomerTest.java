package org.whispersystems.textsecuregcm.subscriptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProcessorCustomerTest {

  @Test
  void toDynamoBytes() {
    final ProcessorCustomer processorCustomer = new ProcessorCustomer("Test", PaymentProvider.BRAINTREE);

    assertArrayEquals(new byte[] { PaymentProvider.BRAINTREE.getId(), 'T', 'e', 's', 't' },
        processorCustomer.toDynamoBytes());
  }
}
