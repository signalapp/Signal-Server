/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;

class RegistrationRequestTest {

  @ParameterizedTest
  @MethodSource
  void isEverySignedKeyValid(final RegistrationRequest registrationRequest, final boolean expectEverySignedKeyValid) {
    assertEquals(expectEverySignedKeyValid, registrationRequest.isEverySignedKeyValid("test"));
  }

  private static List<Arguments> isEverySignedKeyValid() {
    final ECKeyPair aciKeyPair = ECKeyPair.generate();
    final ECKeyPair pniKeyPair = ECKeyPair.generate();

    final IdentityKey aciIdentityKey = new IdentityKey(aciKeyPair.getPublicKey());
    final IdentityKey pniIdentityKey = new IdentityKey(pniKeyPair.getPublicKey());

    final ECSignedPreKey aciEcSignedPreKey = KeysHelper.signedECPreKey(1, aciKeyPair);
    final ECSignedPreKey pniEcSignedPreKey = KeysHelper.signedECPreKey(2, pniKeyPair);

    final KEMSignedPreKey aciKemSignedPreKey = KeysHelper.signedKEMPreKey(3, aciKeyPair);
    final KEMSignedPreKey pniKemSignedPreKey = KeysHelper.signedKEMPreKey(4, pniKeyPair);

    return List.of(
        Arguments.argumentSet("All keys specified",
            buildRegistrationRequestWithKeys(aciIdentityKey, pniIdentityKey, aciEcSignedPreKey, pniEcSignedPreKey, aciKemSignedPreKey, pniKemSignedPreKey),
            true),

        Arguments.argumentSet("Only ACI keys specified",
            buildRegistrationRequestWithKeys(aciIdentityKey, null, aciEcSignedPreKey, null, aciKemSignedPreKey, null),
            true),

        Arguments.argumentSet("ACI EC pre-key missing",
            buildRegistrationRequestWithKeys(aciIdentityKey, pniIdentityKey, null, pniEcSignedPreKey, aciKemSignedPreKey, pniKemSignedPreKey),
            false),

        Arguments.argumentSet("ACI KEM pre-key missing",
            buildRegistrationRequestWithKeys(aciIdentityKey, pniIdentityKey, aciEcSignedPreKey, pniEcSignedPreKey, null, pniKemSignedPreKey),
            false),

        Arguments.argumentSet("PNI identity key missing",
            buildRegistrationRequestWithKeys(aciIdentityKey, null, aciEcSignedPreKey, pniEcSignedPreKey, aciKemSignedPreKey, pniKemSignedPreKey),
            false),

        Arguments.argumentSet("PNI EC pre-key missing",
            buildRegistrationRequestWithKeys(aciIdentityKey, pniIdentityKey, aciEcSignedPreKey, null, aciKemSignedPreKey, pniKemSignedPreKey),
            false),

        Arguments.argumentSet("PNI KEM pre-identity key missing",
            buildRegistrationRequestWithKeys(aciIdentityKey, pniIdentityKey, aciEcSignedPreKey, pniEcSignedPreKey, aciKemSignedPreKey, null),
            false),

        Arguments.argumentSet("Incorrect ACI signature",
            buildRegistrationRequestWithKeys(new IdentityKey(ECKeyPair.generate().getPublicKey()), pniIdentityKey, aciEcSignedPreKey, pniEcSignedPreKey, aciKemSignedPreKey, pniKemSignedPreKey),
            false),

        Arguments.argumentSet("Incorrect PNI signature",
            buildRegistrationRequestWithKeys(aciIdentityKey, new IdentityKey(ECKeyPair.generate().getPublicKey()), aciEcSignedPreKey, pniEcSignedPreKey, aciKemSignedPreKey, pniKemSignedPreKey),
            false)
    );
  }

  private static RegistrationRequest buildRegistrationRequestWithKeys(@Nullable final IdentityKey aciIdentityKey,
      @Nullable final IdentityKey pniIdentityKey,
      @Nullable final ECSignedPreKey aciEcSignedPreKey,
      @Nullable final ECSignedPreKey pniEcSignedPreKey,
      @Nullable final KEMSignedPreKey aciKemSignedPreKey,
      @Nullable final KEMSignedPreKey pniKemSignedPreKey) {

    return new RegistrationRequest(null,
        null,
        null,
        null,
        true,
        aciIdentityKey,
        pniIdentityKey,
        new DeviceActivationRequest(aciEcSignedPreKey,
            Optional.ofNullable(pniEcSignedPreKey),
            aciKemSignedPreKey,
            Optional.ofNullable(pniKemSignedPreKey),
            Optional.empty(),
            Optional.empty()));
  }

  @ParameterizedTest
  @MethodSource
  void isExactlyOneMessageDeliveryChannel(final RegistrationRequest registrationRequest, final boolean expectExactlyOneMessageDeliveryChannel) {
    assertEquals(expectExactlyOneMessageDeliveryChannel, registrationRequest.isExactlyOneMessageDeliveryChannel());
  }

  private static List<Arguments> isExactlyOneMessageDeliveryChannel() {
    return List.of(
        Arguments.argumentSet("Fetches messages",
            buildRegistrationRequestWithDeliveryChannel(true, null, null), true),

        Arguments.argumentSet("APNs token",
            buildRegistrationRequestWithDeliveryChannel(false, "apns", null), true),

        Arguments.argumentSet("FCM token",
            buildRegistrationRequestWithDeliveryChannel(false, null, "fcm"), true),

        Arguments.argumentSet("Multiple delivery channels",
            buildRegistrationRequestWithDeliveryChannel(true, "apns", "fcm"), false),

        Arguments.argumentSet("No delivery channel",
            buildRegistrationRequestWithDeliveryChannel(false, null, null), false)
    );
  };

  private static RegistrationRequest buildRegistrationRequestWithDeliveryChannel(final boolean fetchesMessages,
      @Nullable final String apnsToken,
      @Nullable final String fcmToken) {

    return new RegistrationRequest(null,
        null,
        null,
        new AccountAttributes(fetchesMessages, 1, 2, null, null, false, Collections.emptySet(), null),
        true,
        null,
        null,
        new DeviceActivationRequest(null,
            Optional.empty(),
            null,
            Optional.empty(),
            Optional.ofNullable(apnsToken).map(ApnRegistrationId::new),
            Optional.ofNullable(fcmToken).map(GcmRegistrationId::new)));
  }

  @CartesianTest
  void isAllOrNoPhoneNumberInformationProvided(@CartesianTest.Values(booleans = {true, false}) final boolean pniIdentityKeyPresent,
      @CartesianTest.Values(booleans = {true, false}) final boolean pniRegistrationIdPresent,
      @CartesianTest.Values(booleans = {true, false}) final boolean pniEcSignedPreKeyPresent,
      @CartesianTest.Values(booleans = {true, false}) final boolean pniKemSignedPreKeyPresent) {

    final ECKeyPair pniKeyPair = ECKeyPair.generate();
    final IdentityKey pniIdentityKey = new IdentityKey(pniKeyPair.getPublicKey());
    final ECSignedPreKey pniEcSignedPreKey = KeysHelper.signedECPreKey(2, pniKeyPair);
    final KEMSignedPreKey pniKemSignedPreKey = KeysHelper.signedKEMPreKey(4, pniKeyPair);

    final RegistrationRequest registrationRequest = new RegistrationRequest(null,
        null,
        null,
        new AccountAttributes(true, 1, pniRegistrationIdPresent ? 2 : null, null, null, false, Collections.emptySet(), null),
        true,
        null,
        pniIdentityKeyPresent ? pniIdentityKey : null,
        new DeviceActivationRequest(null,
            pniEcSignedPreKeyPresent ? Optional.of(pniEcSignedPreKey) : Optional.empty(),
            null,
            pniKemSignedPreKeyPresent ? Optional.of(pniKemSignedPreKey) : Optional.empty(),
            Optional.empty(),
            Optional.empty()));

    final boolean expectAllPresentOrAbsent =
        (pniIdentityKeyPresent && pniRegistrationIdPresent && pniEcSignedPreKeyPresent && pniKemSignedPreKeyPresent) ||
            !(pniIdentityKeyPresent || pniRegistrationIdPresent || pniEcSignedPreKeyPresent || pniKemSignedPreKeyPresent);

    assertEquals(expectAllPresentOrAbsent, registrationRequest.isAllOrNoPhoneNumberInformationProvided());
  }
}
