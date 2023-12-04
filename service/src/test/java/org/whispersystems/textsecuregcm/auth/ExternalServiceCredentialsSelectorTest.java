/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsSelector.CredentialInfo;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.MutableClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

public class ExternalServiceCredentialsSelectorTest {

  private static final UUID UUID1 = UUID.randomUUID();
  private static final UUID UUID2 = UUID.randomUUID();
  private static final MutableClock CLOCK = MockUtils.mutableClock(TimeUnit.DAYS.toSeconds(1));

  private static final ExternalServiceCredentialsGenerator GEN1 =
      ExternalServiceCredentialsGenerator
          .builder(TestRandomUtil.nextBytes(32))
          .prependUsername(true)
          .withClock(CLOCK)
          .build();

  private static final ExternalServiceCredentialsGenerator GEN2 =
      ExternalServiceCredentialsGenerator
          .builder(TestRandomUtil.nextBytes(32))
          .withUserDerivationKey(TestRandomUtil.nextBytes(32))
          .prependUsername(false)
          .withDerivedUsernameTruncateLength(16)
          .withClock(CLOCK)
          .build();

  private static ExternalServiceCredentials atTime(
      final ExternalServiceCredentialsGenerator gen,
      final long deltaMillis,
      final UUID identity) {
    final Instant old = CLOCK.instant();
    try {
      CLOCK.incrementMillis(deltaMillis);
      return gen.generateForUuid(identity);
    } finally {
      CLOCK.setTimeInstant(old);
    }
  }

  private static String token(final ExternalServiceCredentials cred) {
    return cred.username() + ":" + cred.password();
  }

  @Test
  void single() {
    final ExternalServiceCredentials cred = GEN1.generateForUuid(UUID1);
    var result = ExternalServiceCredentialsSelector.check(
        List.of(token(cred)), GEN1, TimeUnit.MINUTES.toSeconds(1));
    assertThat(result).singleElement()
        .matches(CredentialInfo::valid)
        .matches(info -> info.credentials().equals(cred));
  }

  @Test
  void multipleUsernames() {
    final ExternalServiceCredentials cred1New = GEN1.generateForUuid(UUID1);
    final ExternalServiceCredentials cred1Old = atTime(GEN1, -1, UUID1);

    final ExternalServiceCredentials cred2New = GEN1.generateForUuid(UUID2);
    final ExternalServiceCredentials cred2Old = atTime(GEN1, -1, UUID2);

    final List<String> tokens = Stream.of(cred1New, cred1Old, cred2New, cred2Old)
        .map(ExternalServiceCredentialsSelectorTest::token)
        .toList();

    final List<CredentialInfo> result = ExternalServiceCredentialsSelector.check(tokens, GEN1,
        TimeUnit.MINUTES.toSeconds(1));
    assertThat(result).hasSize(4);
    assertThat(result).filteredOn(CredentialInfo::valid)
        .hasSize(2)
        .map(CredentialInfo::credentials)
        .containsExactlyInAnyOrder(cred1New, cred2New);
    assertThat(result).filteredOn(info -> !info.valid())
        .map(CredentialInfo::token)
        .containsExactlyInAnyOrder(token(cred1Old), token(cred2Old));
  }

  @Test
  void multipleGenerators() {
    final ExternalServiceCredentials gen1Cred = GEN1.generateForUuid(UUID1);
    final ExternalServiceCredentials gen2Cred = GEN2.generateForUuid(UUID1);

    final List<CredentialInfo> result = ExternalServiceCredentialsSelector.check(
        List.of(token(gen1Cred), token(gen2Cred)),
        GEN2,
        TimeUnit.MINUTES.toSeconds(1));

    assertThat(result)
        .hasSize(2)
        .filteredOn(CredentialInfo::valid)
        .singleElement()
        .matches(info -> info.credentials().equals(gen2Cred));

    assertThat(result)
        .filteredOn(info -> !info.valid())
        .singleElement()
        .matches(info -> info.token().equals(token(gen1Cred)));
  }

  @ParameterizedTest
  @MethodSource
  void invalidCredentials(final String invalidCredential) {
    final ExternalServiceCredentials validCredential = GEN1.generateForUuid(UUID1);
    var result = ExternalServiceCredentialsSelector.check(
        List.of(invalidCredential, token(validCredential)), GEN1, TimeUnit.MINUTES.toSeconds(1));
    assertThat(result).hasSize(2);
    assertThat(result).filteredOn(CredentialInfo::valid).singleElement()
        .matches(info -> info.credentials().equals(validCredential));
    assertThat(result).filteredOn(info -> !info.valid()).singleElement()
        .matches(info -> info.token().equals(invalidCredential));
  }

  static Stream<String> invalidCredentials() {
    return Stream.of(
        "blah:blah",
        token(atTime(GEN1, -TimeUnit.MINUTES.toSeconds(2), UUID1)), // too old
        "nocolon",
        "nothingaftercolon:",
        ":nothingbeforecolon",
        token(GEN2.generateForUuid(UUID1))
    );
  }

}
