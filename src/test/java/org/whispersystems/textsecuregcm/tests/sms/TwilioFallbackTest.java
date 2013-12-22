package org.whispersystems.textsecuregcm.tests.sms;

import com.google.common.base.Optional;
import com.twilio.sdk.TwilioRestException;
import junit.framework.TestCase;
import org.whispersystems.textsecuregcm.sms.NexmoSmsSender;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sms.TwilioSmsSender;

import java.io.IOException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class TwilioFallbackTest extends TestCase {

  private NexmoSmsSender  nexmoSender  = mock(NexmoSmsSender.class );
  private TwilioSmsSender twilioSender = mock(TwilioSmsSender.class);

  @Override
  protected void setUp() throws IOException, TwilioRestException {
    doThrow(new TwilioRestException("foo", 404)).when(twilioSender).deliverSmsVerification(anyString(), anyString());
    doThrow(new TwilioRestException("bar", 405)).when(twilioSender).deliverVoxVerification(anyString(), anyString());
  }

  public void testNexmoSmsFallback() throws IOException, TwilioRestException {
    SmsSender smsSender = new SmsSender(twilioSender, Optional.of(nexmoSender), true);
    smsSender.deliverSmsVerification("+442223334444", "123-456");

    verify(nexmoSender).deliverSmsVerification("+442223334444", "123-456");
    verify(twilioSender).deliverSmsVerification("+442223334444", "123-456");
  }

  public void testNexmoVoxFallback() throws IOException, TwilioRestException {
    SmsSender smsSender = new SmsSender(twilioSender, Optional.of(nexmoSender), true);
    smsSender.deliverVoxVerification("+442223334444", "123-456");

    verify(nexmoSender).deliverVoxVerification("+442223334444", "123-456");
    verify(twilioSender).deliverVoxVerification("+442223334444", "123-456");
  }


}
