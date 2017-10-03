package org.whispersystems.textsecuregcm.tests.util;

import com.amazonaws.HttpMethod;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.AttachmentsConfiguration;
import org.whispersystems.textsecuregcm.s3.UrlSigner;

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UrlSignerTest {

  @Test
  public void testTransferAcceleration() {
    AttachmentsConfiguration configuration = mock(AttachmentsConfiguration.class);
    when(configuration.getAccessKey()).thenReturn("foo");
    when(configuration.getAccessSecret()).thenReturn("bar");
    when(configuration.getBucket()).thenReturn("attachments-test");

    UrlSigner signer = new UrlSigner(configuration);
    URL url = signer.getPreSignedUrl(1234, HttpMethod.GET, false);

    assertThat(url).hasHost("attachments-test.s3-accelerate.amazonaws.com");
  }

  @Test
  public void testTransferUnaccelerated() {
    AttachmentsConfiguration configuration = mock(AttachmentsConfiguration.class);
    when(configuration.getAccessKey()).thenReturn("foo");
    when(configuration.getAccessSecret()).thenReturn("bar");
    when(configuration.getBucket()).thenReturn("attachments-test");

    UrlSigner signer = new UrlSigner(configuration);
    URL url = signer.getPreSignedUrl(1234, HttpMethod.GET, true);

    assertThat(url).hasHost("s3.amazonaws.com");
  }

}
