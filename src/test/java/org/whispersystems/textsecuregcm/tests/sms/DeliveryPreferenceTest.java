package org.whispersystems.textsecuregcm.tests.sms;

import com.google.common.base.Optional;
import com.twilio.sdk.TwilioRestException;
import junit.framework.TestCase;
import org.whispersystems.textsecuregcm.sms.NexmoSmsSender;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sms.TwilioSmsSender;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class DeliveryPreferenceTest extends TestCase {

  private TwilioSmsSender twilioSender = mock(TwilioSmsSender.class);
  private NexmoSmsSender  nexmoSender  = mock(NexmoSmsSender.class);

  public void testInternationalPreferenceOff() throws IOException, TwilioRestException {
    SmsSender smsSender = new SmsSender(twilioSender, Optional.of(nexmoSender), false);

    smsSender.deliverSmsVerification("+441112223333", "123-456");
    verify(nexmoSender).deliverSmsVerification("+441112223333", "123-456");
    verifyNoMoreInteractions(twilioSender);
  }

  public void testInternationalPreferenceOn() throws IOException, TwilioRestException {
    SmsSender smsSender = new SmsSender(twilioSender, Optional.of(nexmoSender), true);

    smsSender.deliverSmsVerification("+441112223333", "123-456");
    verify(twilioSender).deliverSmsVerification("+441112223333", "123-456");
    verifyNoMoreInteractions(nexmoSender);
  }
}
