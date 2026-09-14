/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth.webauthn;

import com.google.common.annotations.VisibleForTesting;
import com.webauthn4j.WebAuthnManager;
import com.webauthn4j.credential.CredentialRecord;
import com.webauthn4j.data.AuthenticationData;
import com.webauthn4j.data.AuthenticationParameters;
import com.webauthn4j.data.PublicKeyCredentialParameters;
import com.webauthn4j.data.PublicKeyCredentialType;
import com.webauthn4j.data.RegistrationData;
import com.webauthn4j.data.RegistrationParameters;
import com.webauthn4j.data.RegistrationRequest;
import com.webauthn4j.data.attestation.statement.COSEAlgorithmIdentifier;
import com.webauthn4j.data.client.Origin;
import com.webauthn4j.server.ServerProperty;
import com.webauthn4j.util.exception.WebAuthnException;
import com.webauthn4j.verifier.exception.BadChallengeException;
import com.webauthn4j.verifier.exception.VerificationException;
import io.lettuce.core.SetArgs;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.util.HmacUtils;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import tools.jackson.core.JacksonException;

/// Handles WebAuthn registration and authentication operations.
public class WebAuthnCeremonyManager {
  /// [relying party ID - WebAuthn TR](https://www.w3.org/TR/webauthn/#relying-party-identifier)
  private final String rpId;
  /// [origin - WebAuthn TR](https://www.w3.org/TR/webauthn/#dom-collectedclientdata-origin)
  private final Origin origin;
  private final byte[] blindingSecret;
  private final Duration challengeTtl;

  private final FaultTolerantRedisClusterClient challengeStorageCluster;
  private final WebAuthnManager webAuthnManager;

  private static final String RETRY_NAME = ResilienceUtil.name(WebAuthnCeremonyManager.class);

  /// Ed25519 support is fairly limited, while ES256 is broadly supported
  private static final List<PublicKeyCredentialParameters> ALLOWED_WEBAUTHN_CREDENTIAL_ALGORITHMS = List.of(
      new PublicKeyCredentialParameters(PublicKeyCredentialType.PUBLIC_KEY, COSEAlgorithmIdentifier.Ed25519),
      new PublicKeyCredentialParameters(PublicKeyCredentialType.PUBLIC_KEY, COSEAlgorithmIdentifier.ES256)
  );

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  public WebAuthnCeremonyManager(final String rpId, final String origin, final Duration challengeTtl, final byte[] blindingSecret,
                                 final FaultTolerantRedisClusterClient challengeStorageCluster) {
    this.rpId = rpId;
    this.origin = new Origin(origin);
    this.challengeTtl = challengeTtl;
    this.blindingSecret = blindingSecret;
    this.challengeStorageCluster = challengeStorageCluster;
    this.webAuthnManager = WebAuthnManager.createNonStrictWebAuthnManager();
  }

  /// Initiates a WebAuthn registration ceremony
  ///
  /// @param identifier The account identifier
  ///
  /// @param existingCredentials a list of already-stored credential records for the user; their IDs
  /// will be provided to the client to help prevent registering a duplicate ID on the same
  /// authenticator
  ///
  /// @return parameters the client must use for its step in the registration ceremony. `identifier` will be blinded, so
  /// that it is a [non-identifiable user handle](https://www.w3.org/TR/webauthn/#sctn-user-handle-privacy)
  public RegistrationCeremonyParameters startRegistration(final UUID identifier, final List<? extends CredentialRecord> existingCredentials) {
    return new RegistrationCeremonyParameters(
        blindIdentifier(identifier),
        ALLOWED_WEBAUTHN_CREDENTIAL_ALGORITHMS.stream().map(pkcp -> pkcp.getAlg().getValue()).toList(),
        existingCredentials.stream().map(credentialRecord -> credentialRecord.getAttestedCredentialData().getCredentialId()).toList()
    );
  }

  /// Verifies that the supplied authenticator response components form a
  /// valid WebAuthn registration ceremony with acceptable parameters.
  ///
  /// @param serializedAttestationObject the "attestation object" from the authenticator response,
  /// serialized in the format specified by the WebAuthn TR. Its attestation format must be `none`
  /// and any attestation statement must have been cleared to mitigate privacy risks.
  ///
  /// @param collectedClientDataJson the "collected client data" from the authenticator response,
  /// serialized in the [format specified by the WebAuthn TR](https://www.w3.org/TR/webauthn/#sctn-generating-an-attestation-object)
  ///
  /// @return a record that can be stored for future authentication.
  ///
  /// @throws WebAuthnException if the supplied authenticator response is
  /// invalid for any reason (structurally or due to selecting unacceptable
  /// parameters, for example an unsupported signature algorithm)
  public RegistrationCeremonyResult verifyRegistration(
    final byte[] serializedAttestationObject, final String collectedClientDataJson) throws WebAuthnException {

    final RegistrationData registrationData;
    try {
      registrationData = webAuthnManager.parse(
          new RegistrationRequest(serializedAttestationObject, collectedClientDataJson.getBytes(StandardCharsets.UTF_8)));
    } catch (final JacksonException e) {
      throw new WebAuthnException(e);
    }

    // These shouldn't ever be null, because the upstream RPC inputs are not nullable. However, the fields are annotated as
    // nullable.
    if (registrationData.getAttestationObject() == null) {
      throw new WebAuthnException("Attestation object must not be null");
    }
    if (registrationData.getCollectedClientData() == null) {
      throw new WebAuthnException("Collected client data must not be null");
    }

    if (!"none".equals(registrationData.getAttestationObject().getAttestationStatement().getFormat())) {
      // Refuse any attestations; clients must clear them to prevent sharing information we don't want to store
      throw new WebAuthnException("attestation format must be 'none'");
    }

    final ServerProperty serverProperties = ServerProperty.builder()
        // we aren't verifying any signatures for registration, so we don't need to bother saving a
        // challenge; accept whatever is supplied
        .challenge(registrationData.getCollectedClientData().getChallenge())
        .origin(origin)
        .rpId(rpId)
        .build();

    webAuthnManager.verify(registrationData,
        new RegistrationParameters(serverProperties, ALLOWED_WEBAUTHN_CREDENTIAL_ALGORITHMS, false, true));

    return new RegistrationCeremonyResult(
        registrationData.getAttestationObject().getAuthenticatorData().getAttestedCredentialData(),
        registrationData.getCollectedClientData(),
        registrationData.getAttestationObject().getAuthenticatorData().getSignCount());
  }

  /// Initiates a WebAuthn authentication ceremony.
  ///
  /// Each call generates, and replaces any existing, [challenge](https://www.w3.org/TR/webauthn/#sctn-cryptographic-challenges).
  ///
  /// @param identifier the identifier of the account to be authenticated
  /// @param existingCredentials a list of stored credential records for the user being authenticated
  ///
  /// @return a [AuthenticationCeremonyParameters] record with the details
  /// needed by a client in possession of a matching authenticator to complete
  /// the ceremony
  public AuthenticationCeremonyParameters startAuthentication(final UUID identifier, List<? extends CredentialRecord> existingCredentials) {
    final byte[] challengeKey = challengeKey(identifier);
    final byte[] challenge = new byte[16];
    SECURE_RANDOM.nextBytes(challenge);

    ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
        .executeRunnable(
            () -> challengeStorageCluster.withBinaryCluster(cluster -> cluster.sync().set(challengeKey, challenge, SetArgs.Builder.ex(challengeTtl.toSeconds()))));

    return new AuthenticationCeremonyParameters(
        challenge,
        challengeTtl,
        existingCredentials.stream().map(cr -> cr.getAttestedCredentialData().getCredentialId()).toList());
  }

  /// Parses an authenticator response JSON blob, whose format is specified in the WebAuthn Technical Report
  public AuthenticationData parseAuthenticationResponse(final String authenticationResponseJson) throws WebAuthnException {
    try {
      return webAuthnManager.parseAuthenticationResponseJSON(authenticationResponseJson);
    } catch (final NullPointerException | JacksonException e) {
      throw new WebAuthnException(e);
    }
  }

  /// Verifies an authenticator response to a WebAuthn authentication ceremony, consuming any existing stored challenge.
  ///
  /// If verification succeeds, the `CredentialRecord` is [updated in-place](https://www.w3.org/TR/webauthn-3/#authn-ceremony-update-credential-record).
  ///
  /// @param identifier the identifier of the account to be authenticated
  /// @param matchingCredential the stored credential record for the account to be authenticated with an ID matching the one from the authenticator response
  /// @param authenticationData the parsed authenticator response
  ///
  ///
  /// @throws VerificationException if the response could not be verified for any reason (incorrect signature, incorrect or expired challenge)
  public void verifyAuthentication(
      final UUID identifier,
      final CredentialRecord matchingCredential,
      final AuthenticationData authenticationData) throws VerificationException {

    final byte[] challengeKey = challengeKey(identifier);
    @Nullable final byte[] challenge = ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
        .executeSupplier(() -> challengeStorageCluster.withBinaryCluster(cluster -> cluster.sync().getdel(challengeKey)));

    //noinspection ConstantValue
    if (challenge == null) {
      throw new BadChallengeException("no challenge stored");
    }

    final byte[] credentialId = matchingCredential.getAttestedCredentialData().getCredentialId();
    final AuthenticationParameters authenticationParameters = new AuthenticationParameters(
        ServerProperty.builder()
            .origin(origin)
            .rpId(rpId)
            .challenge(() -> challenge)
            .build(),
        matchingCredential,
        List.of(credentialId),
        false,
        true);

    webAuthnManager.verify(authenticationData, authenticationParameters);
  }

  /// @return `true` if [CredentialRecord]s represent the same actual credential ID. Note that this is more specific than
  /// whether they came from the same authenticator device, which cannot be determined in general.
  public static boolean isSameCredential(final CredentialRecord a, final CredentialRecord b) {
    return Arrays.equals(a.getAttestedCredentialData().getCredentialId(), b.getAttestedCredentialData().getCredentialId());
  }

  @VisibleForTesting
  static byte[] challengeKey(final UUID identifier) {
    return String.format("webauthn_challenge::{%s}", identifier).getBytes(StandardCharsets.UTF_8);
  }

  private byte[] blindIdentifier(final UUID identifier) {
    return HmacUtils.hmac256Truncated(blindingSecret, UUIDUtil.toBytes(identifier), 32);
  }
}
