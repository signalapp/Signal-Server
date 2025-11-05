/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.security.KeyStore;
import java.time.Instant;
import java.util.Map;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.security.CertificateUtils;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.jetty.TestResource;

class TlsCertificateExpirationUtilTest {

  // Please note that this certificate is used only for testing and is not used anywhere outside of this test.
  // It was generated with:
  //
  // ```shell
  // #!/bin/bash
  //
  // export PASSWORD=test123
  //
  // openssl req -newkey rsa:2048 -keyout test-root.key -nodes -x509 -days 36500 -out test-root.crt \
  //     -subj "/CN=test-root" \
  //     -addext "basicConstraints=critical,CA:TRUE"
  //
  // openssl req -new -newkey rsa:2048 -keyout test-rsa.key -out test-rsa.csr -passout env:PASSWORD \
  //     -subj "/CN=localhost" \
  //     -addext "subjectAltName = DNS:localhost"
  //
  // openssl req -new -newkey ED25519 -keyout test-ed25519.key -out test-ed25519.csr -passout env:PASSWORD \
  //     -subj "/CN=localhost" \
  //     -addext "subjectAltName = DNS:localhost"
  //
  // openssl x509 -req  -CAkey test-root.key -CA test-root.crt -days 36500  -copy_extensions copyall \
  //     -in test-rsa.csr \
  //     -out test-rsa.crt
  //
  // # create unique timestamps
  // sleep 3
  //
  // openssl x509 -req  -CAkey test-root.key -CA test-root.crt -days 36500  -copy_extensions copyall \
  //     -in test-ed25519.csr \
  //     -out test-ed25519.crt
  //
  // cat test-root.crt >> test-rsa.crt
  //
  // openssl pkcs12 -export -in test-ed25519.crt -inkey test-ed25519.key -name 'ed25519' -out keystore.p12 \
  //     -passin env:PASSWORD -passout env:PASSWORD
  //
  // openssl pkcs12 -export -in test-rsa.crt -inkey test-rsa.key -name 'rsa' -out keystore-rsa.p12 \
  //     -passin env:PASSWORD -passout env:PASSWORD
  //
  // keytool -importkeystore -noprompt \
  //     -srckeystore keystore-rsa.p12 \
  //     -srcstoretype PKCS12 \
  //     -srcstorepass $PASSWORD \
  //     -destkeystore keystore.p12 \
  //     -deststoretype PKCS12 \
  //     -deststorepass $PASSWORD
  //
  // base64 -b 80 -i keystore.p12
  // ```
  private static final String KEYSTORE_BASE64 = """
      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
      N6VjWLZbEYq1VlM8QpdcaaoCAggA
      """;
  private static final String KEYSTORE_PASSWORD = "test123";

  private static final Instant EDDSA_EXPIRATION = Instant.ofEpochSecond(4897215163L);
  private static final Instant RSA_EXPIRATION = Instant.ofEpochSecond(4897215160L);

  @Test
  void test() throws Exception {
    try (Resource keystore = TestResource.fromBase64Mime("keystore", KEYSTORE_BASE64)) {

      final KeyStore keyStore = CertificateUtils.getKeyStore(keystore, "PKCS12", null, KEYSTORE_PASSWORD);

      final Map<String, Instant> expected = Map.of(
          "localhost:EdDSA", EDDSA_EXPIRATION,
          "localhost:RSA", RSA_EXPIRATION);
      assertEquals(expected, TlsCertificateExpirationUtil.getIdentifiersAndExpirations(keyStore, KEYSTORE_PASSWORD));
    }
  }

}
