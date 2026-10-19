/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage.devicecheck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.webauthn4j.appattest.DeviceCheckManager;
import com.webauthn4j.appattest.data.DCAssertionData;
import com.webauthn4j.appattest.data.DCAssertionRequest;
import com.webauthn4j.converter.exception.DataConversionException;
import com.webauthn4j.converter.util.ObjectConverter;
import com.webauthn4j.data.attestation.authenticator.AuthenticatorData;
import java.nio.ByteBuffer;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AppAttestAssertionConverterTest {

  private static final ObjectConverter OBJECT_CONVERTER = DeviceCheckManager.createObjectConverter();

  @ParameterizedTest
  @ValueSource(bytes = {AuthenticatorData.BIT_AT, AuthenticatorData.BIT_ED, AuthenticatorData.BIT_AT | AuthenticatorData.BIT_ED})
  void parseBadAssertion(byte flags) {
    final DCAssertionData data = AppAttestAssertionConverter.parseAppleAssertion(
        new DCAssertionRequest(new byte[32], createAssertion(flags), new byte[32]));

    assertThat(data.getAuthenticatorDataBytes()).isEqualTo(createAuthenticatorData(flags));
    assertThat(data.getAuthenticatorData().getFlags()).isEqualTo(flags);
    assertThat(data.getAuthenticatorData().getSignCount()).isEqualTo(32);
    assertThat(data.getAuthenticatorData().getAttestedCredentialData()).isNull();
    assertThat(data.getAuthenticatorData().getExtensions().getKeys())
        .containsExactlyInAnyOrder("apple_bundle_version_01", "apple_validation_category_01");
  }

  // If this test fails, we have updated to a version of webauthn4j that handles misformatted attestations, and
  // the helper class can be removed.
  @Test
  void webauthn4jRejectsIOS27Assertion() {
    final DeviceCheckManager deviceCheckManager = new DeviceCheckManager(new AppleDeviceCheckTrustAnchor());
    final byte[] appAttestAssertion = createAssertion((byte) (AuthenticatorData.BIT_AT | AuthenticatorData.BIT_ED));
    assertThatExceptionOfType(DataConversionException.class).isThrownBy(() ->
        deviceCheckManager.parse(new DCAssertionRequest(new byte[32], appAttestAssertion, new byte[32])));
  }

  /// An assertion that has extensions and no attestedCredentialData but potentially with mismatched flags
  private static byte[] createAssertion(final byte flags) {
    return OBJECT_CONVERTER.getCborMapper().writeValueAsBytes(Map.of(
        "signature", new byte[64],
        "authenticatorData", createAuthenticatorData(flags)));
  }

  private static byte[] createAuthenticatorData(final byte flags) {
    final byte[] extensions = OBJECT_CONVERTER.getCborMapper().writeValueAsBytes(Map.of(
        "apple_bundle_version_01", "1",
        "apple_validation_category_01", new byte[]{2, 0, 0, 0}));

    return ByteBuffer.allocate(37 + extensions.length)
        .put(new byte[32]) // rpIdHash
        .put((byte) flags)  // flags
        .putInt(32)  // signCount
        .put(extensions)
        .array();
  }
}
