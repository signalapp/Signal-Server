/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage.devicecheck;

import com.webauthn4j.appattest.DeviceCheckManager;
import com.webauthn4j.appattest.data.DCAssertion;
import com.webauthn4j.appattest.data.DCAssertionData;
import com.webauthn4j.appattest.data.DCAssertionRequest;
import com.webauthn4j.converter.AuthenticatorDataConverter;
import com.webauthn4j.converter.exception.DataConversionException;
import com.webauthn4j.converter.util.ObjectConverter;
import com.webauthn4j.data.attestation.authenticator.AuthenticatorData;
import com.webauthn4j.data.extension.authenticator.AuthenticationExtensionAuthenticatorOutput;
import tools.jackson.core.JacksonException;
import java.util.Arrays;

/// Parses Apple App Attest assertions
///
/// App Attest assertions since iOS 27 include an extensions map and don't include attested credential data. They
/// set the AT flag which indicates data, so a standards-compliant parser barfs because there's an extensions map where
/// the AT data should/ be.
///
/// If Apple fixes their headers or webauthn4j adopts some workaround, this class should be removed
///
/// [webauthn4j #1497](https://github.com/webauthn4j/webauthn4j/issues/1497)
/// [webauthn4j #1518](https://github.com/webauthn4j/webauthn4j/pull/1518)
class AppAttestAssertionConverter {

  // https://www.w3.org/TR/webauthn/#sctn-authenticator-data
  // rpIdHash(32) || flags(1) || signCount(4) || attestedCredentialData(variable) || extensions(variable)
  private static final int FLAGS_OFFSET = 32;
  private static final int HEADER_LENGTH = 37;

  // https://www.rfc-editor.org/info/rfc8949/#section-3.1:~:text=the%20CBOR%20Encoding-,3.1%20Major%20Types,-3.2%20Indefinite%20Lengths
  // CBOR major type 5 (map), covering both definite and indefinite length maps
  private static final int CBOR_MAJOR_TYPE_MASK = 0xe0;
  private static final int CBOR_MAP_MAJOR_TYPE = 0xa0;

  private static final ObjectConverter OBJECT_CONVERTER = DeviceCheckManager.createObjectConverter();
  private static final AuthenticatorDataConverter AUTHENTICATOR_DATA_CONVERTER = new AuthenticatorDataConverter(OBJECT_CONVERTER);

  /// Parse an assertion request that potentially has misconfigured FLAGS. The returned data must still be verified.
  static DCAssertionData parseAppleAssertion(final DCAssertionRequest request) throws DataConversionException {
    final DCAssertion assertion = OBJECT_CONVERTER.getCborMapper().readValue(request.getAssertion(), DCAssertion.class);
    final byte[] authenticatorDataBytes = assertion == null ? null : assertion.getAuthenticatorData();
    final AuthenticatorData<AuthenticationExtensionAuthenticatorOutput> authenticatorData =
        authenticatorDataBytes == null ? null : convertAuthenticatorData(authenticatorDataBytes);
    return new DCAssertionData(request.getKeyId(), authenticatorData, authenticatorDataBytes,
        request.getClientDataHash(), assertion == null ? null : assertion.getSignature());
  }

  private static AuthenticatorData<AuthenticationExtensionAuthenticatorOutput> convertAuthenticatorData(final byte[] original) {
    if (!hasAppleAssertionExtensions(original)) {
      return AUTHENTICATOR_DATA_CONVERTER.convert(original);
    }

    final AuthenticatorData<AuthenticationExtensionAuthenticatorOutput> parsed;
    try {
      // Create a copy just for parsing and set the flags as Apple should have set them
      final byte[] modified = Arrays.copyOf(original, original.length);
      modified[FLAGS_OFFSET] = (byte) ((original[FLAGS_OFFSET] & ~AuthenticatorData.BIT_AT) | AuthenticatorData.BIT_ED);
      parsed = AUTHENTICATOR_DATA_CONVERTER.convert(modified);
    } catch (final JacksonException _) {
      // Jackson error messages can include the (untrusted) extension contents
      throw new DataConversionException("Invalid App Attest assertion extension data");
    }

    // Return the parsed structures but keep the original (incorrect) flags, so signature validation works
    return new AuthenticatorData<>(parsed.getRpIdHash(), original[FLAGS_OFFSET], parsed.getSignCount(),
        parsed.getExtensions());
  }

  private static boolean hasAppleAssertionExtensions(final byte[] data) {
    // If we don't have AT or Extensions at all, webauthn can handle them being incorrectly set without any conversion
    return data.length > HEADER_LENGTH
        // If the data correctly sets the AT flag, webauthn can also handle it
        && AuthenticatorData.checkFlagAT(data[FLAGS_OFFSET])
        // If the first byte after the header looks like an extension, this is the special case. Valid assertions
        // shouldn't have attestedCredentialData anyway, and if it did it should be a CBOR string
        && (data[HEADER_LENGTH] & CBOR_MAJOR_TYPE_MASK) == CBOR_MAP_MAJOR_TYPE;
  }
}
