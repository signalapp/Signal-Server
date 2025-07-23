/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64WithPaddingAdapter;

public record DeviceInfo(long id,

                         @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
                         @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
                         byte[] name,

                         long lastSeen,

                         @Deprecated
                         @Schema(description = """
                             The time in milliseconds since epoch when the device was linked.
                             Deprecated in favor of `createdAtCiphertext`.
                         """, deprecated = true)
                         long created,

                         @Schema(description = "The registration ID of the given device.")
                         int registrationId,

                         @JsonSerialize(using = ByteArrayAdapter.Serializing.class)
                         @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
                         @Schema(description = """
                             The ciphertext of the time in milliseconds since epoch when the device was attached
                             to the parent account, encoded in standard base64 without padding.
                             """)
                         byte[] createdAtCiphertext) {

  public static DeviceInfo forDevice(final Device device) {
    return new DeviceInfo(device.getId(), device.getName(), device.getLastSeen(), device.getCreated(), device.getRegistrationId(
        IdentityType.ACI), device.getCreatedAtCiphertext());
  }
}
