/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.auth;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;

class ExternalServiceCredentialsGeneratorTest {

  @Test
  void testGenerateDerivedUsername() {
    final ExternalServiceCredentialsGenerator generator = ExternalServiceCredentialsGenerator
        .builder(new byte[32])
        .withUserDerivationKey(new byte[32])
        .build();
    final ExternalServiceCredentials credentials = generator.generateFor("+14152222222");

    assertThat(credentials.username()).isNotEqualTo("+14152222222");
    assertThat(credentials.password().startsWith("+14152222222")).isFalse();
  }

  @Test
  void testGenerateNoDerivedUsername() {
    final ExternalServiceCredentialsGenerator generator = ExternalServiceCredentialsGenerator
        .builder(new byte[32])
        .build();
    final ExternalServiceCredentials credentials = generator.generateFor("+14152222222");

    assertThat(credentials.username()).isEqualTo("+14152222222");
    assertThat(credentials.password().startsWith("+14152222222")).isTrue();
  }

}
