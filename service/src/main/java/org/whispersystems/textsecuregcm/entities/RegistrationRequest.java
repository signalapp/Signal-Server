/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import javax.annotation.Nullable;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.IdentityKeyAdapter;

public record RegistrationRequest(@Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  The ID of an existing verification session as it appears in a verification session
                                  metadata object. Must be provided if `recoveryPassword` and `receiptCredentialPresentation`
                                  are not provided; must not be provided if `recoveryPassword` or `receiptCredentialPresentation`
                                  is provided.
                                  """)
                                  @Nullable
                                  String sessionId,

                                  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                                  @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  A base64-encoded registration recovery password. Must be provided if `sessionId` and
                                  `receiptCredentialPresentation` are not provided; must not be provided if `sessionId`
                                  or `receiptCredentialPresentation` is provided.
                                  """)
                                  @Nullable
                                  byte[] recoveryPassword,

                                  @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                                  @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  A base64-encoded receipt credential presentation that allows for redeeming a Signal Login.
                                  Must be provided if `sessionId` and `recoveryPassword` are not provided;
                                  must not be provided if `sessionId` or `recoveryPassword` is provided.
                                  """)
                                  @Nullable
                                  byte[] receiptCredentialPresentation,

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

                                  @NotNull
                                  @Valid
                                  @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = """
                                  The ACI-associated identity key for the account, encoded as a base64 string.
                                  """)
                                  @JsonSerialize(using = IdentityKeyAdapter.Serializer.class)
                                  @JsonDeserialize(using = IdentityKeyAdapter.Deserializer.class)
                                  IdentityKey aciIdentityKey,

                                  @Valid
                                  @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  The PNI-associated identity key for the account, encoded as a base64 string.
                                  Must be provided for an account with a phone number.
                                  """)
                                  @JsonSerialize(using = IdentityKeyAdapter.Serializer.class)
                                  @JsonDeserialize(using = IdentityKeyAdapter.Deserializer.class)
                                  @Nullable
                                  IdentityKey pniIdentityKey,

                                  @NotNull
                                  @Valid
                                  @JsonUnwrapped
                                  @JsonProperty
                                  DeviceActivationRequest deviceActivationRequest) implements PhoneVerificationRequest {

  public boolean isEverySignedKeyValid(@Nullable final String userAgent) {
    if (deviceActivationRequest().aciSignedPreKey() == null ||
        deviceActivationRequest().aciPqLastResortPreKey() == null) {
      return false;
    }

    final boolean allPniPresentOrAllAbsent = ((pniIdentityKey == null) == deviceActivationRequest().pniSignedPreKey().isEmpty())
        && ((pniIdentityKey == null) == deviceActivationRequest().pniPqLastResortPreKey().isEmpty());
    if (!allPniPresentOrAllAbsent) {
      return false;
    }

    return PreKeySignatureValidator.validatePreKeySignatures(aciIdentityKey(), List.of(deviceActivationRequest().aciSignedPreKey(), deviceActivationRequest().aciPqLastResortPreKey()), userAgent, "register")
        && (pniIdentityKey == null || PreKeySignatureValidator.validatePreKeySignatures(pniIdentityKey(), List.of(deviceActivationRequest().pniSignedPreKey().get(), deviceActivationRequest().pniPqLastResortPreKey().get()), userAgent, "register"));
  }

  @VisibleForTesting
  @AssertTrue
  @Schema(hidden = true)
  boolean isExactlyOneMessageDeliveryChannel() {
    if (deviceActivationRequest == null || accountAttributes == null) {
      return false;
    }
    if (accountAttributes.getFetchesMessages()) {
      return deviceActivationRequest().apnToken().isEmpty() && deviceActivationRequest().gcmToken().isEmpty();
    } else {
      return deviceActivationRequest().apnToken().isPresent() ^ deviceActivationRequest().gcmToken().isPresent();
    }
  }
}
