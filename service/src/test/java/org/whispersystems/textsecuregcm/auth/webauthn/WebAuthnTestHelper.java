/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.webauthn;

import com.webauthn4j.data.AttestationConveyancePreference;
import com.webauthn4j.data.AuthenticatorAssertionResponse;
import com.webauthn4j.data.AuthenticatorAttestationResponse;
import com.webauthn4j.data.AuthenticatorSelectionCriteria;
import com.webauthn4j.data.PublicKeyCredential;
import com.webauthn4j.data.PublicKeyCredentialCreationOptions;
import com.webauthn4j.data.PublicKeyCredentialDescriptor;
import com.webauthn4j.data.PublicKeyCredentialParameters;
import com.webauthn4j.data.PublicKeyCredentialRequestOptions;
import com.webauthn4j.data.PublicKeyCredentialRpEntity;
import com.webauthn4j.data.PublicKeyCredentialType;
import com.webauthn4j.data.PublicKeyCredentialUserEntity;
import com.webauthn4j.data.ResidentKeyRequirement;
import com.webauthn4j.data.UserVerificationRequirement;
import com.webauthn4j.data.attestation.statement.COSEAlgorithmIdentifier;
import com.webauthn4j.data.client.Origin;
import com.webauthn4j.data.client.challenge.Challenge;
import com.webauthn4j.data.client.challenge.DefaultChallenge;
import com.webauthn4j.test.authenticator.webauthn.WebAuthnAuthenticator;
import com.webauthn4j.test.authenticator.webauthn.WebAuthnAuthenticatorAdaptor;
import com.webauthn4j.test.client.ClientPlatform;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import javax.annotation.Nullable;

class WebAuthnTestHelper {

  private static final AuthenticatorSelectionCriteria AUTHENTICATOR_SELECTION = new AuthenticatorSelectionCriteria(
      null, false, ResidentKeyRequirement.DISCOURAGED, UserVerificationRequirement.DISCOURAGED);

  private final ClientPlatform clientPlatform;
  private final String rpId;

  TestRegistrationData register(RegistrationCeremonyParameters registrationParameters) {
    // the webauthn4j emulator can only produce EC and RSA credentials, so Ed25519 isn't an option
    return register(registrationParameters.userHandle(), COSEAlgorithmIdentifier.ES256);
  }

  /// Data a client uses to complete a registration ceremony.
  record TestRegistrationData(byte[] serializedAttestationObject, String collectedClientDataJson, byte[] credentialId) {
  }

  WebAuthnTestHelper(final String rpId, final String origin, final WebAuthnAuthenticator authenticator) {
    this.rpId = rpId;
    this.clientPlatform = new ClientPlatform(new Origin(origin), new WebAuthnAuthenticatorAdaptor(authenticator));
  }

  TestRegistrationData register(final byte[] userHandle, final COSEAlgorithmIdentifier algorithm) {
    return register(userHandle, algorithm, new DefaultChallenge(), AttestationConveyancePreference.NONE);
  }

  TestRegistrationData register(
      final byte[] userHandle,
      final COSEAlgorithmIdentifier algorithm,
      final Challenge challenge,
      final AttestationConveyancePreference attestationConveyancePreference) {
    final PublicKeyCredential<AuthenticatorAttestationResponse, ?> credential =
        clientPlatform.create(new PublicKeyCredentialCreationOptions(
            new PublicKeyCredentialRpEntity(rpId, "test"),
            new PublicKeyCredentialUserEntity(userHandle, "user", "user"),
            challenge,
            List.of(new PublicKeyCredentialParameters(PublicKeyCredentialType.PUBLIC_KEY, algorithm)),
            null,
            List.of(),
            AUTHENTICATOR_SELECTION,
            attestationConveyancePreference,
            null));

    return new TestRegistrationData(
        credential.getResponse().getAttestationObject(),
        new String(credential.getResponse().getClientDataJSON(), StandardCharsets.UTF_8),
        credential.getRawId());
  }

  /// Produces the JSON-serialized assertion response a client would send to conclude an authentication ceremony.
  String authenticate(final byte[] credentialId, final byte[] challenge) {
    final PublicKeyCredential<AuthenticatorAssertionResponse, ?> credential =
        clientPlatform.get(new PublicKeyCredentialRequestOptions(
            new DefaultChallenge(challenge),
            null,
            rpId,
            List.of(new PublicKeyCredentialDescriptor(PublicKeyCredentialType.PUBLIC_KEY, credentialId, null)),
            UserVerificationRequirement.DISCOURAGED,
            null));

    return authenticationResponseJson(credential);
  }

  /// Produces an assertion response whose signature has been tampered with.
  String authenticateWithCorruptedSignature(final byte[] credentialId, final byte[] challenge) {
    final PublicKeyCredential<AuthenticatorAssertionResponse, ?> credential =
        clientPlatform.get(new PublicKeyCredentialRequestOptions(
            new DefaultChallenge(challenge),
            null,
            rpId,
            List.of(new PublicKeyCredentialDescriptor(PublicKeyCredentialType.PUBLIC_KEY, credentialId, null)),
            UserVerificationRequirement.DISCOURAGED,
            null));

    final AuthenticatorAssertionResponse response = credential.getResponse();
    final byte[] corruptedSignature = response.getSignature().clone();
    corruptedSignature[corruptedSignature.length - 1] ^= 0xff;

    return authenticationResponseJson(new PublicKeyCredential<>(
        credential.getRawId(),
        new AuthenticatorAssertionResponse(
            response.getClientDataJSON(), response.getAuthenticatorData(), corruptedSignature, response.getUserHandle()),
        credential.getClientExtensionResults()));
  }

  /// Serializes an assertion into the [specification](https://www.w3.org/TR/webauthn/#dictdef-authenticationresponsejson) format.
  static String authenticationResponseJson(final PublicKeyCredential<AuthenticatorAssertionResponse, ?> credential) {
    final AuthenticatorAssertionResponse response = credential.getResponse();

    return """
        {
          "id": "%s",
          "rawId": "%s",
          "type": "public-key",
          "clientExtensionResults": {},
          "response": {
            "clientDataJSON": "%s",
            "authenticatorData": "%s",
            "signature": "%s",
            "userHandle": %s
          }
        }
        """.formatted(
        base64Url(credential.getRawId()),
        base64Url(credential.getRawId()),
        base64Url(response.getClientDataJSON()),
        base64Url(response.getAuthenticatorData()),
        base64Url(response.getSignature()),
        response.getUserHandle() == null ? "null" : "\"" + base64Url(response.getUserHandle()) + "\"");
  }

  private static String base64Url(@Nullable final byte[] bytes) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }
}
