/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage.devicecheck;

import static org.mockito.Mockito.mockStatic;

import com.webauthn4j.appattest.DeviceCheckManager;
import com.webauthn4j.appattest.authenticator.DCAppleDevice;
import com.webauthn4j.appattest.authenticator.DCAppleDeviceImpl;
import com.webauthn4j.appattest.data.DCAttestationData;
import com.webauthn4j.appattest.data.DCAttestationParameters;
import com.webauthn4j.appattest.data.DCAttestationRequest;
import com.webauthn4j.appattest.server.DCServerProperty;
import com.webauthn4j.data.attestation.AttestationObject;
import com.webauthn4j.data.client.challenge.DefaultChallenge;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class DeviceCheckTestUtil {

  // https://www.apple.com/certificateauthority/Apple_App_Attestation_Root_CA.pem
  private static final String APPLE_APP_ATTEST_ROOT = """
      -----BEGIN CERTIFICATE-----
      MIICITCCAaegAwIBAgIQC/O+DvHN0uD7jG5yH2IXmDAKBggqhkjOPQQDAzBSMSYw
      JAYDVQQDDB1BcHBsZSBBcHAgQXR0ZXN0YXRpb24gUm9vdCBDQTETMBEGA1UECgwK
      QXBwbGUgSW5jLjETMBEGA1UECAwKQ2FsaWZvcm5pYTAeFw0yMDAzMTgxODMyNTNa
      Fw00NTAzMTUwMDAwMDBaMFIxJjAkBgNVBAMMHUFwcGxlIEFwcCBBdHRlc3RhdGlv
      biBSb290IENBMRMwEQYDVQQKDApBcHBsZSBJbmMuMRMwEQYDVQQIDApDYWxpZm9y
      bmlhMHYwEAYHKoZIzj0CAQYFK4EEACIDYgAERTHhmLW07ATaFQIEVwTtT4dyctdh
      NbJhFs/Ii2FdCgAHGbpphY3+d8qjuDngIN3WVhQUBHAoMeQ/cLiP1sOUtgjqK9au
      Yen1mMEvRq9Sk3Jm5X8U62H+xTD3FE9TgS41o0IwQDAPBgNVHRMBAf8EBTADAQH/
      MB0GA1UdDgQWBBSskRBTM72+aEH/pwyp5frq5eWKoTAOBgNVHQ8BAf8EBAMCAQYw
      CgYIKoZIzj0EAwMDaAAwZQIwQgFGnByvsiVbpTKwSga0kP0e8EeDS4+sQmTvb7vn
      53O5+FRXgeLhpJ06ysC5PrOyAjEAp5U4xDgEgllF7En3VcE3iexZZtKeYnpqtijV
      oyFraWVIyd/dganmrduC1bmTBGwD
      -----END CERTIFICATE-----
      """;

  // Sample attestation from apple docs:
  // https://developer.apple.com/documentation/devicecheck/attestation-object-validation-guide#Example-setup
  final static String APPLE_SAMPLE_TEAM_ID = "0352187391";
  final static String APPLE_SAMPLE_BUNDLE_ID = "com.apple.example_app_attest";
  final static String APPLE_SAMPLE_CHALLENGE = "test_server_challenge";
  final static byte[] APPLE_SAMPLE_KEY_ID = Base64.getDecoder().decode("bSrEhF8TIzIvWSPwvZ0i2+UOBre4ASH84rK15m6emNY=");
  final static byte[] APPLE_SAMPLE_ATTESTATION = loadBinaryResource("apple-sample-attestation");
  // Leaf certificate in apple sample attestation expires 2024-04-20
  final static Instant APPLE_SAMPLE_TIME = Instant.parse("2024-04-19T00:00:00.00Z");

  // Sample attestation from webauthn4j:
  // https://github.com/webauthn4j/webauthn4j/blob/6b7a8f8edce4ab589c49ecde8740873ab96c4218/webauthn4j-appattest/src/test/java/com/webauthn4j/appattest/DeviceCheckManagerTest.java#L126
  final static String SAMPLE_TEAM_ID = "8YE23NZS57";
  final static String SAMPLE_BUNDLE_ID = "com.kayak.travel";
  final static byte[] SAMPLE_KEY_ID = Base64.getDecoder().decode("VnfqjSp0rWyyqNhrfh+9/IhLIvXuYTPAmJEVQwl4dko=");
  final static String SAMPLE_CHALLENGE = "1234567890abcdefgh"; // same challenge used for the attest and assert
  final static byte[] SAMPLE_ASSERTION = loadBinaryResource("webauthn4j-sample-assertion");
  final static byte[] SAMPLE_ATTESTATION = loadBinaryResource("webauthn4j-sample-attestation");
  // Leaf certificate in sample attestation expires 2020-09-30
  final static Instant SAMPLE_TIME = Instant.parse("2020-09-28T00:00:00Z");


  public static DeviceCheckManager appleDeviceCheckManager() {
    return new DeviceCheckManager(new AppleDeviceCheckTrustAnchor());
  }

  public static DCAppleDevice sampleDevice() {
    final byte[] clientDataHash = sha256(SAMPLE_CHALLENGE.getBytes(StandardCharsets.UTF_8));
    return validate(SAMPLE_CHALLENGE, clientDataHash, SAMPLE_KEY_ID, SAMPLE_ATTESTATION, SAMPLE_TEAM_ID,
        SAMPLE_BUNDLE_ID, SAMPLE_TIME);
  }

  public static DCAppleDevice appleSampleDevice() {
    // Note: the apple example provides the clientDataHash (typically the SHA256 of the challenge), NOT the challenge,
    // despite them referring to the value as a challenge
    final byte[] clientDataHash = APPLE_SAMPLE_CHALLENGE.getBytes(StandardCharsets.UTF_8);

    return validate(APPLE_SAMPLE_CHALLENGE, clientDataHash, APPLE_SAMPLE_KEY_ID, APPLE_SAMPLE_ATTESTATION,
        APPLE_SAMPLE_TEAM_ID, APPLE_SAMPLE_BUNDLE_ID, APPLE_SAMPLE_TIME);
  }

  private static DCAppleDevice validate(final String challengePlainText, final byte[] clientDataHash,
      final byte[] keyId, final byte[] attestation, final String teamId, final String bundleId, final Instant now) {

    final DCAttestationRequest dcAttestationRequest = new DCAttestationRequest(keyId, attestation, clientDataHash);

    final DCAttestationData dcAttestationData;
    try (final MockedStatic<Instant> instantMock = mockStatic(Instant.class, Mockito.CALLS_REAL_METHODS)) {
      instantMock.when(Instant::now).thenReturn(now);

      dcAttestationData = appleDeviceCheckManager().validate(dcAttestationRequest, new DCAttestationParameters(
          new DCServerProperty(
              teamId, bundleId,
              new DefaultChallenge(challengePlainText.getBytes(StandardCharsets.UTF_8)))));
    }

    final AttestationObject attestationObject = dcAttestationData.getAttestationObject();
    return new DCAppleDeviceImpl(
        attestationObject.getAuthenticatorData().getAttestedCredentialData(),
        attestationObject.getAttestationStatement(),
        attestationObject.getAuthenticatorData().getSignCount(),
        attestationObject.getAuthenticatorData().getExtensions());
  }

  private static byte[] sha256(byte[] bytes) {
    final MessageDigest sha256;
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      // All Java implementations are required to support SHA-256
      throw new AssertionError(e);
    }
    return sha256.digest(bytes);
  }

  private static byte[] loadBinaryResource(final String resourceName) {
    try (InputStream stream = DeviceCheckTestUtil.class.getResourceAsStream(resourceName)) {
      if (stream == null) {
        throw new IllegalArgumentException("Resource not found: " + resourceName);
      }
      return stream.readAllBytes();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
