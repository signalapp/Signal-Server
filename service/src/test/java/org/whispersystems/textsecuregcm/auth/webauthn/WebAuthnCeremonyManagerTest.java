/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.webauthn;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.webauthn4j.data.AttestationConveyancePreference;
import com.webauthn4j.data.AuthenticationData;
import com.webauthn4j.data.attestation.statement.COSEAlgorithmIdentifier;
import com.webauthn4j.data.client.challenge.DefaultChallenge;
import com.webauthn4j.test.EmulatorUtil;
import com.webauthn4j.util.exception.WebAuthnException;
import com.webauthn4j.verifier.exception.BadChallengeException;
import com.webauthn4j.verifier.exception.BadOriginException;
import com.webauthn4j.verifier.exception.BadRpIdException;
import com.webauthn4j.verifier.exception.VerificationException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.auth.webauthn.WebAuthnTestHelper.TestRegistrationData;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.storage.AnnotatedWebAuthnCredential;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class WebAuthnCeremonyManagerTest {

  @RegisterExtension
  private static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private static final String RP_ID = "example.org";
  private static final String ORIGIN = "https://example.org";
  private static final Duration CHALLENGE_TTL = Duration.ofMinutes(2);

  private WebAuthnCeremonyManager webAuthnCeremonyManager;
  private WebAuthnTestHelper helper;

  @BeforeEach
  void setUp() {
    webAuthnCeremonyManager = new WebAuthnCeremonyManager(RP_ID, ORIGIN, CHALLENGE_TTL, TestRandomUtil.nextBytes(32),
        REDIS_CLUSTER_EXTENSION.getRedisCluster());
    helper = new WebAuthnTestHelper(RP_ID, ORIGIN, EmulatorUtil.NONE_ATTESTATION_AUTHENTICATOR);
  }

  /// Creates a [AnnotatedWebAuthnCredential] from the result with a random metadata ciphertext
  private static AnnotatedWebAuthnCredential createCredential(final RegistrationCeremonyResult registrationResult) {
    return new AnnotatedWebAuthnCredential(
        registrationResult.attestedCredentialData(),
        registrationResult.signCount(),
        TestRandomUtil.nextBytes(160));
  }

  @Nested
  class Registration {

    @Test
    void verifyRegistration() {
      final RegistrationCeremonyParameters registrationParameters = webAuthnCeremonyManager.startRegistration(
          UUID.randomUUID(), Collections.emptyList());
      
      final TestRegistrationData regData = helper.register(registrationParameters);

      final RegistrationCeremonyResult result = webAuthnCeremonyManager
          .verifyRegistration(regData.serializedAttestationObject(), regData.collectedClientDataJson());

      assertArrayEquals(regData.credentialId(), result.attestedCredentialData().getCredentialId());
      assertEquals(ORIGIN, result.collectedClientData().getOrigin().toString());
    }

    @Test
    void startRegistrationAdvertisesAllowedAlgorithms() {
      final RegistrationCeremonyParameters parameters =
          webAuthnCeremonyManager.startRegistration(UUID.randomUUID(), List.of());

      assertEquals(
          List.of(COSEAlgorithmIdentifier.Ed25519.getValue(), COSEAlgorithmIdentifier.ES256.getValue()),
          parameters.allowedAlgorithms());
    }

    @Test
    void startRegistrationExcludesExistingCredentials() {
      final UUID identifier = UUID.randomUUID();
      final TestRegistrationData firstRegData = helper.register(webAuthnCeremonyManager.startRegistration(identifier, List.of()));
      final AnnotatedWebAuthnCredential firstCredential = createCredential(webAuthnCeremonyManager.verifyRegistration(
          firstRegData.serializedAttestationObject(), firstRegData.collectedClientDataJson()));

      final TestRegistrationData secondRegData = helper.register(webAuthnCeremonyManager.startRegistration(identifier, List.of()));
      final AnnotatedWebAuthnCredential secondCredential = createCredential(webAuthnCeremonyManager.verifyRegistration(
          secondRegData.serializedAttestationObject(), secondRegData.collectedClientDataJson()));

      final RegistrationCeremonyParameters parameters =
          webAuthnCeremonyManager.startRegistration(identifier, List.of(firstCredential, secondCredential));

      assertEquals(2, parameters.excludedCredentialIds().size());
      assertArrayEquals(firstCredential.getCredentialId(), parameters.excludedCredentialIds().getFirst());
      assertArrayEquals(secondCredential.getCredentialId(), parameters.excludedCredentialIds().getLast());
    }

    @Test
    void verifyRegistrationRejectsAttestation() {
      final WebAuthnTestHelper helper = new WebAuthnTestHelper(RP_ID, ORIGIN, EmulatorUtil.PACKED_AUTHENTICATOR);

      final byte[] userHandle = webAuthnCeremonyManager.startRegistration(UUID.randomUUID(), List.of()).userHandle();

      final TestRegistrationData regData = helper.register(userHandle,
          COSEAlgorithmIdentifier.ES256, new DefaultChallenge(), AttestationConveyancePreference.DIRECT);

      assertThrows(WebAuthnException.class, () -> webAuthnCeremonyManager.verifyRegistration(
          regData.serializedAttestationObject(), regData.collectedClientDataJson()));
    }

    @Test
    void verifyRegistrationRejectsDisallowedAlgorithm() {
      final byte[] userHandle = webAuthnCeremonyManager.startRegistration(UUID.randomUUID(), List.of()).userHandle();

      final TestRegistrationData regData = helper.register(userHandle, COSEAlgorithmIdentifier.PS256);

      assertThrows(WebAuthnException.class, () -> webAuthnCeremonyManager.verifyRegistration(
          regData.serializedAttestationObject(), regData.collectedClientDataJson()));
    }

    @Test
    void verifyRegistrationRejectsWrongOrigin() {
      final WebAuthnTestHelper helper = new WebAuthnTestHelper(RP_ID, "https://not-signal.example.com",
          EmulatorUtil.NONE_ATTESTATION_AUTHENTICATOR);

      final TestRegistrationData regData = helper.register(webAuthnCeremonyManager.startRegistration(UUID.randomUUID(), List.of()));

      assertThrows(BadOriginException.class, () -> webAuthnCeremonyManager.verifyRegistration(
          regData.serializedAttestationObject(), regData.collectedClientDataJson()));
    }

    @Test
    void verifyRegistrationRejectsWrongRpId() {
      final WebAuthnTestHelper helper = new WebAuthnTestHelper("not-signal.example.com", ORIGIN,
          EmulatorUtil.NONE_ATTESTATION_AUTHENTICATOR);

      final TestRegistrationData regData = helper.register(webAuthnCeremonyManager.startRegistration(UUID.randomUUID(), List.of()));

      assertThrows(BadRpIdException.class, () -> webAuthnCeremonyManager.verifyRegistration(
          regData.serializedAttestationObject(), regData.collectedClientDataJson()));
    }

    @Test
    void verifyRegistrationRejectsMalformedAttestationObject() {
      final TestRegistrationData regData = helper.register(webAuthnCeremonyManager.startRegistration(UUID.randomUUID(), List.of()));

      assertThrows(WebAuthnException.class, () -> webAuthnCeremonyManager.verifyRegistration(
          TestRandomUtil.nextBytes(64), regData.collectedClientDataJson()));
    }

    @Test
    void verifyRegistrationRejectsMalformedClientDataJson() {
      final TestRegistrationData regData = helper.register(webAuthnCeremonyManager.startRegistration(UUID.randomUUID(), List.of()));

      assertThrows(WebAuthnException.class, () -> webAuthnCeremonyManager.verifyRegistration(
          regData.serializedAttestationObject(), "this is not json"));
    }
  }

  @Nested
  class Authentication {

    private UUID identifier;
    private AnnotatedWebAuthnCredential credential;

    @BeforeEach
    void setUp() {
      identifier = UUID.randomUUID();
      final TestRegistrationData regData = helper.register(webAuthnCeremonyManager.startRegistration(UUID.randomUUID(), List.of()));

      credential = createCredential(webAuthnCeremonyManager.verifyRegistration(
          regData.serializedAttestationObject(), regData.collectedClientDataJson()));
    }


    @Test
    void startAuthenticationStoresChallenge() {
      final AuthenticationCeremonyParameters parameters =
          webAuthnCeremonyManager.startAuthentication(identifier, List.of(credential));

      assertEquals(16, parameters.challenge().length);
      assertEquals(CHALLENGE_TTL, parameters.timeout());
      assertEquals(1, parameters.allowedCredentialIds().size());
      assertArrayEquals(credential.getCredentialId(), parameters.allowedCredentialIds().getFirst());

      final byte[] storedChallenge = REDIS_CLUSTER_EXTENSION.getRedisCluster()
          .withBinaryCluster(cluster -> cluster.sync().get(WebAuthnCeremonyManager.challengeKey(identifier)));

      assertArrayEquals(parameters.challenge(), storedChallenge);

      final long ttlMillis = REDIS_CLUSTER_EXTENSION.getRedisCluster()
          .withBinaryCluster(cluster -> cluster.sync().pttl(WebAuthnCeremonyManager.challengeKey(identifier)));

      assertTrue(ttlMillis > 0 && ttlMillis <= CHALLENGE_TTL.toMillis(),
          "Expected challenge TTL within " + CHALLENGE_TTL + ", but was " + Duration.ofMillis(ttlMillis));
    }

    @Test
    void startAuthenticationReplacesPreviousChallenge() {
      final byte[] firstChallenge = webAuthnCeremonyManager.startAuthentication(identifier, List.of(credential)).challenge();
      final byte[] secondChallenge = webAuthnCeremonyManager.startAuthentication(identifier, List.of(credential)).challenge();

      assertFalse(Arrays.equals(firstChallenge, secondChallenge));

      final AuthenticationData staleAuthenticationData = webAuthnCeremonyManager.parseAuthenticationResponse(
          helper.authenticate(credential.getCredentialId(), firstChallenge));

      assertThrows(BadChallengeException.class,
          () -> webAuthnCeremonyManager.verifyAuthentication(identifier, credential, staleAuthenticationData));
    }

    @Test
    void verifyAuthentication() throws VerificationException {
      final byte[] challenge = webAuthnCeremonyManager.startAuthentication(identifier, List.of(credential)).challenge();

      final AuthenticationData authenticationData = webAuthnCeremonyManager.parseAuthenticationResponse(
          helper.authenticate(credential.getCredentialId(), challenge));

      assertArrayEquals(credential.getCredentialId(), authenticationData.getCredentialId());

      final long preVerificationSignCount = credential.getCounter();

      webAuthnCeremonyManager.verifyAuthentication(identifier, credential, authenticationData);

      assertEquals(preVerificationSignCount + 1, credential.getCounter());
    }

    @Test
    void verifyAuthenticationConsumesChallenge() throws VerificationException {
      final byte[] challenge = webAuthnCeremonyManager.startAuthentication(identifier, List.of(credential)).challenge();

      final AuthenticationData authenticationData = webAuthnCeremonyManager.parseAuthenticationResponse(
          helper.authenticate(credential.getCredentialId(), challenge));

      webAuthnCeremonyManager.verifyAuthentication(identifier, credential, authenticationData);

      // Replaying the same assertion must fail; the challenge is consumed on first use
      assertThrows(BadChallengeException.class,
          () -> webAuthnCeremonyManager.verifyAuthentication(identifier, credential, authenticationData));

      assertEquals(0L, REDIS_CLUSTER_EXTENSION.getRedisCluster()
          .withBinaryCluster(cluster -> cluster.sync().exists(WebAuthnCeremonyManager.challengeKey(identifier))).longValue());
    }

    @Test
    void verifyAuthenticationNoStoredChallenge() {
      final AuthenticationData authenticationData = webAuthnCeremonyManager.parseAuthenticationResponse(
          helper.authenticate(credential.getCredentialId(), TestRandomUtil.nextBytes(16)));

      assertThrows(BadChallengeException.class,
          () -> webAuthnCeremonyManager.verifyAuthentication(identifier, credential, authenticationData));
    }

    @Test
    void verifyAuthenticationExpiredChallenge() {
      final byte[] challenge = webAuthnCeremonyManager.startAuthentication(identifier, List.of(credential)).challenge();

      final AuthenticationData authenticationData = webAuthnCeremonyManager.parseAuthenticationResponse(
          helper.authenticate(credential.getCredentialId(), challenge));

      // simulate expiration by deleting the challenge
      assertEquals(1, (long) REDIS_CLUSTER_EXTENSION.getRedisCluster()
          .withBinaryCluster(cluster -> cluster.sync().del(WebAuthnCeremonyManager.challengeKey(identifier))));

      assertThrows(BadChallengeException.class,
          () -> webAuthnCeremonyManager.verifyAuthentication(identifier, credential, authenticationData));
    }

    @Test
    void verifyAuthenticationWrongChallenge() {
      webAuthnCeremonyManager.startAuthentication(identifier, List.of(credential));

      final AuthenticationData authenticationData = webAuthnCeremonyManager.parseAuthenticationResponse(
          helper.authenticate(credential.getCredentialId(), TestRandomUtil.nextBytes(16)));

      assertThrows(BadChallengeException.class,
          () -> webAuthnCeremonyManager.verifyAuthentication(identifier, credential, authenticationData));
    }

    @Test
    void verifyAuthenticationMismatchedCredential() {
      final UUID identifier = UUID.randomUUID();

      final TestRegistrationData firstRegData = helper.register(
          webAuthnCeremonyManager.startRegistration(identifier, List.of()));
      final AnnotatedWebAuthnCredential firstCredential = createCredential(webAuthnCeremonyManager.verifyRegistration(
          firstRegData.serializedAttestationObject(), firstRegData.collectedClientDataJson()));

      final TestRegistrationData secondRegData = helper.register(
          webAuthnCeremonyManager.startRegistration(identifier, List.of()));
      final AnnotatedWebAuthnCredential secondCredential = createCredential(webAuthnCeremonyManager.verifyRegistration(
          secondRegData.serializedAttestationObject(), secondRegData.collectedClientDataJson()));

      final byte[] challenge = webAuthnCeremonyManager.startAuthentication(identifier, List.of(firstCredential)).challenge();

      final AuthenticationData authenticationData = webAuthnCeremonyManager.parseAuthenticationResponse(
          helper.authenticate(secondCredential.getCredentialId(), challenge));

      assertThrows(VerificationException.class,
          () -> webAuthnCeremonyManager.verifyAuthentication(identifier, firstCredential, authenticationData),
          "the second authenticator must not verify against the first credential");
    }

    @Test
    void verifyAuthenticationBadSignature() {
      final byte[] challenge = webAuthnCeremonyManager.startAuthentication(identifier, List.of(credential)).challenge();

      final AuthenticationData authenticationData = webAuthnCeremonyManager.parseAuthenticationResponse(
          helper.authenticateWithCorruptedSignature(credential.getCredentialId(), challenge));

      assertThrows(VerificationException.class,
          () -> webAuthnCeremonyManager.verifyAuthentication(identifier, credential, authenticationData));
    }

    @Test
    void verifyAuthenticationWrongOrigin() {
      final byte[] challenge = webAuthnCeremonyManager.startAuthentication(identifier, List.of(credential)).challenge();

      final WebAuthnTestHelper wrongOrigin =
          new WebAuthnTestHelper(RP_ID, "https://not-signal.example.com", EmulatorUtil.NONE_ATTESTATION_AUTHENTICATOR);
      final TestRegistrationData incorrectOriginData = wrongOrigin.register(webAuthnCeremonyManager.startRegistration(identifier, List.of()));

      final AuthenticationData authenticationData = webAuthnCeremonyManager.parseAuthenticationResponse(
          wrongOrigin.authenticate(incorrectOriginData.credentialId(), challenge));

      assertThrows(VerificationException.class,
          () -> webAuthnCeremonyManager.verifyAuthentication(identifier, credential, authenticationData));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "this is not json",
        "[]",
        "\"qux\"",
        "null",
        "{}",
        """
                {"id":"baz"}
            """,
        """
                {"id":"foo","rawId":"bar","type":"public-key","response":{}}
            """})
    void parseAuthenticationResponseRejectsMalformedJson(final String authenticationResponseJson) {
      assertThrows(WebAuthnException.class,
          () -> webAuthnCeremonyManager.parseAuthenticationResponse(authenticationResponseJson));
    }


    @Test
    void isSameCredential() {
      final TestRegistrationData regData = helper.register(webAuthnCeremonyManager.startRegistration(identifier, List.of()));
      final RegistrationCeremonyResult registrationResult = webAuthnCeremonyManager.verifyRegistration(
          regData.serializedAttestationObject(), regData.collectedClientDataJson());

      final AnnotatedWebAuthnCredential credential = createCredential(registrationResult);

      // different stored metadata
      final AnnotatedWebAuthnCredential sameCredential = createCredential(registrationResult);
      // …and counter
      sameCredential.setCounter(42);

      final TestRegistrationData otherRegData = helper.register(
          webAuthnCeremonyManager.startRegistration(identifier, List.of()));
      final AnnotatedWebAuthnCredential otherCredential = createCredential(webAuthnCeremonyManager.verifyRegistration(
          otherRegData.serializedAttestationObject(), otherRegData.collectedClientDataJson()));

      // isSameCredential solely compares credentialId
      assertTrue(WebAuthnCeremonyManager.isSameCredential(credential, sameCredential));
      assertTrue(WebAuthnCeremonyManager.isSameCredential(credential, credential));
      assertFalse(WebAuthnCeremonyManager.isSameCredential(credential, otherCredential));

      assertNotEquals(credential, sameCredential, "same credential IDs with different counters are not equal");
    }
  }
}
