package org.whispersystems.textsecuregcm.entities;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.Valid;

import org.whispersystems.textsecuregcm.util.ValidPreKey;
import org.whispersystems.textsecuregcm.util.ValidPreKey.PreKeyType;

import java.util.Optional;

public record DeviceActivationRequest(@Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  A signed EC pre-key to be associated with this account's ACI. If provided, an account
                                  will be created "atomically," and all other properties needed for atomic account
                                  creation must also be present.
                                  """)
                                Optional<@Valid @ValidPreKey(type=PreKeyType.ECC) SignedPreKey> aciSignedPreKey,

                                      @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  A signed EC pre-key to be associated with this account's PNI. If provided, an account
                                  will be created "atomically," and all other properties needed for atomic account
                                  creation must also be present.
                                  """)
                                Optional<@Valid @ValidPreKey(type=PreKeyType.ECC) SignedPreKey> pniSignedPreKey,

                                      @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  A signed Kyber-1024 "last resort" pre-key to be associated with this account's ACI. If
                                  provided, an account will be created "atomically," and all other properties needed for
                                  atomic account creation must also be present.
                                  """)
                                Optional<@Valid @ValidPreKey(type=PreKeyType.ECC) SignedPreKey> aciPqLastResortPreKey,

                                      @Schema(requiredMode = Schema.RequiredMode.NOT_REQUIRED, description = """
                                  A signed Kyber-1024 "last resort" pre-key to be associated with this account's PNI. If
                                  provided, an account will be created "atomically," and all other properties needed for
                                  atomic account creation must also be present.
                                  """)
                                Optional<@Valid @ValidPreKey(type=PreKeyType.ECC) SignedPreKey> pniPqLastResortPreKey,

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
                                Optional<@Valid GcmRegistrationId> gcmToken) {
}
