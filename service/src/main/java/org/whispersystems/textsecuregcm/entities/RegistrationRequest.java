/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.annotations.VisibleForTesting;
import io.swagger.v3.oas.annotations.media.Schema;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

import javax.validation.Valid;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Optional;

public record RegistrationRequest(@Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  The ID of an existing verification session as it appears in a verification session
                                  metadata object. Must be provided if `recoveryPassword` is not provided; must not be
                                  provided if `recoveryPassword` is provided.
                                  """)
                                  String sessionId,

                                  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                                  @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  A base64-encoded registration recovery password. Must be provided if `sessionId` is
                                  not provided; must not be provided if `sessionId` is provided
                                  """)
                                  byte[] recoveryPassword,

                                  @NotNull
                                  @Valid
                                  @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
                                  AccountAttributes accountAttributes,

                                  @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = """
                                  If true, indicates that the end user has elected not to transfer data from another
                                  device even though a device transfer is technically possible given the capabilities of
                                  the calling device and the device associated with the existing account (if any). If
                                  false and if a device transfer is technically possible, the registration request will
                                  fail with an HTTP/409 response indicating that the client should prompt the user to
                                  transfer data from an existing device.
                                  """)
                                  boolean skipDeviceTransfer,

                                  @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  The ACI-associated identity key for the account, encoded as a base64 string. If
                                  provided, an account will be created "atomically," and all other properties needed for
                                  atomic account creation must also be present.
                                  """)
                                  Optional<String> aciIdentityKey,

                                  @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  The PNI-associated identity key for the account, encoded as a base64 string. If
                                  provided, an account will be created "atomically," and all other properties needed for
                                  atomic account creation must also be present.
                                  """)
                                  Optional<String> pniIdentityKey,

                                  @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  A signed EC pre-key to be associated with this account's ACI. If provided, an account
                                  will be created "atomically," and all other properties needed for atomic account
                                  creation must also be present.
                                  """)
                                  Optional<@Valid SignedPreKey> aciSignedPreKey,

                                  @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  A signed EC pre-key to be associated with this account's PNI. If provided, an account
                                  will be created "atomically," and all other properties needed for atomic account
                                  creation must also be present.
                                  """)
                                  Optional<@Valid SignedPreKey> pniSignedPreKey,

                                  @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  A signed Kyber-1024 "last resort" pre-key to be associated with this account's ACI. If
                                  provided, an account will be created "atomically," and all other properties needed for
                                  atomic account creation must also be present.
                                  """)
                                  Optional<@Valid SignedPreKey> aciPqLastResortPreKey,

                                  @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  A signed Kyber-1024 "last resort" pre-key to be associated with this account's PNI. If
                                  provided, an account will be created "atomically," and all other properties needed for
                                  atomic account creation must also be present.
                                  """)
                                  Optional<@Valid SignedPreKey> pniPqLastResortPreKey,

                                  @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  An APNs token set for the account's primary device. If provided, the account's primary
                                  device will be notified of new messages via push notifications to the given token. If
                                  creating an account "atomically," callers must provide exactly one of an APNs token
                                  set, an FCM token, or an `AccountAttributes` entity with `fetchesMessages` set to
                                  `true`.
                                  """)
                                  Optional<@Valid ApnRegistrationId> apnToken,

                                  @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  An FCM/GCM token for the account's primary device. If provided, the account's primary
                                  device will be notified of new messages via push notifications to the given token. If
                                  creating an account "atomically," callers must provide exactly one of an APNs token
                                  set, an FCM token, or an `AccountAttributes` entity with `fetchesMessages` set to
                                  `true`.
                                  """)
                                  Optional<@Valid GcmRegistrationId> gcmToken) implements PhoneVerificationRequest {

  @AssertTrue
  public boolean isEverySignedKeyValid() {
    return validatePreKeySignature(aciIdentityKey(), aciSignedPreKey())
        && validatePreKeySignature(pniIdentityKey(), pniSignedPreKey())
        && validatePreKeySignature(aciIdentityKey(), aciPqLastResortPreKey())
        && validatePreKeySignature(pniIdentityKey(), pniPqLastResortPreKey());
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static boolean validatePreKeySignature(final Optional<String> maybeIdentityKey,
      final Optional<SignedPreKey> maybeSignedPreKey) {

    return maybeSignedPreKey.map(signedPreKey -> maybeIdentityKey
            .map(identityKey -> PreKeySignatureValidator.validatePreKeySignatures(identityKey, List.of(signedPreKey)))
            .orElse(false))
        .orElse(true);
  }

  @AssertTrue
  public boolean isCompleteRequest() {
    final boolean hasNoAtomicAccountCreationParameters =
        aciIdentityKey().isEmpty()
            && pniIdentityKey().isEmpty()
            && aciSignedPreKey().isEmpty()
            && pniSignedPreKey().isEmpty()
            && aciPqLastResortPreKey().isEmpty()
            && pniPqLastResortPreKey().isEmpty();

    return supportsAtomicAccountCreation() || hasNoAtomicAccountCreationParameters;
  }

  public boolean supportsAtomicAccountCreation() {
    return hasExactlyOneMessageDeliveryChannel()
        && aciIdentityKey().isPresent()
        && pniIdentityKey().isPresent()
        && aciSignedPreKey().isPresent()
        && pniSignedPreKey().isPresent()
        && aciPqLastResortPreKey().isPresent()
        && pniPqLastResortPreKey().isPresent();
  }

  @VisibleForTesting
  boolean hasExactlyOneMessageDeliveryChannel() {
    if (accountAttributes.getFetchesMessages()) {
      return apnToken.isEmpty() && gcmToken.isEmpty();
    } else {
      return apnToken.isPresent() ^ gcmToken.isPresent();
    }
  }
}
