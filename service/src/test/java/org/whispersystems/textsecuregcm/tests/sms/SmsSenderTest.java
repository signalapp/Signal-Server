package org.whispersystems.textsecuregcm.tests.sms;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sms.TwilioSmsSender;

class SmsSenderTest {

  private static final String NON_MEXICO_NUMBER        = "+12345678901";
  private static final String MEXICO_NON_MOBILE_NUMBER = "+52234567890";
  private static final String MEXICO_MOBILE_NUMBER     = "+52123456789";

  private final TwilioSmsSender twilioSmsSender = mock(TwilioSmsSender.class);
  private final SmsSender       smsSender       = new SmsSender(twilioSmsSender);

  @Test
  void testDeliverSmsVerificationNonMexico() {
    smsSender.deliverSmsVerification(NON_MEXICO_NUMBER, Optional.empty(), "");
    verify(twilioSmsSender, times(1))
        .deliverSmsVerification(NON_MEXICO_NUMBER, Optional.empty(), "");
  }

  @Test
  void testDeliverSmsVerificationMexicoNonMobile() {
    smsSender.deliverSmsVerification(MEXICO_NON_MOBILE_NUMBER, Optional.empty(), "");
    verify(twilioSmsSender, times(1))
        .deliverSmsVerification("+521" + MEXICO_NON_MOBILE_NUMBER.substring("+52".length()), Optional.empty(), "");
  }

  @Test
  void testDeliverSmsVerificationMexicoMobile() {
    smsSender.deliverSmsVerification(MEXICO_MOBILE_NUMBER, Optional.empty(), "");
    verify(twilioSmsSender, times(1))
        .deliverSmsVerification(MEXICO_MOBILE_NUMBER, Optional.empty(), "");
  }
}
