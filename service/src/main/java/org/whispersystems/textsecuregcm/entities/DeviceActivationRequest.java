package org.whispersystems.textsecuregcm.entities;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public record DeviceActivationRequest(
    @NotNull
    @Valid
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = """
        A signed EC pre-key to be associated with this account's ACI.
        """)
    ECSignedPreKey aciSignedPreKey,

    @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
        If the account has a phone number, a signed EC pre-key to be associated with this account's PNI.
        """)
    Optional<@Valid ECSignedPreKey> pniSignedPreKey,

    @NotNull
    @Valid
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = """
        A signed Kyber-1024 "last resort" pre-key to be associated with this account's ACI.
        """)
    KEMSignedPreKey aciPqLastResortPreKey,

    @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
        If the account has a phone number, a signed Kyber-1024 "last resort" pre-key to be associated with this account's PNI.
        """)
    Optional<@Valid KEMSignedPreKey> pniPqLastResortPreKey,

    @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
        An APNs token set for the account's primary device. If provided, the account's primary
        device will be notified of new messages via push notifications to the given token.
        Callers must provide exactly one of an APNs token set, an FCM token, or an
        `AccountAttributes` entity with `fetchesMessages` set to `true`.
        """)
    Optional<@Valid ApnRegistrationId> apnToken,

    @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
        An FCM/GCM token for the account's primary device. If provided, the account's primary
        device will be notified of new messages via push notifications to the given token.
        Callers must provide exactly one of an APNs token set, an FCM token, or an
        `AccountAttributes` entity with `fetchesMessages` set to `true`.
        """)
    Optional<@Valid GcmRegistrationId> gcmToken) {
}
