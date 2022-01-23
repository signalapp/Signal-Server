/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.HttpMethod;
import java.net.URL;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.s3.UrlSigner;

class UrlSignerTest {

  @Test
  void testTransferAcceleration() {
    UrlSigner signer = new UrlSigner("foo", "bar", "attachments-test");
    URL url = signer.getPreSignedUrl(1234, HttpMethod.GET, false);

    assertThat(url).hasHost("attachments-test.s3-accelerate.amazonaws.com");
  }

  @Test
  void testTransferUnaccelerated() {
    UrlSigner signer = new UrlSigner("foo", "bar", "attachments-test");
    URL url = signer.getPreSignedUrl(1234, HttpMethod.GET, true);

    assertThat(url).hasHost("s3.amazonaws.com");
  }

}
