/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64WithPaddingAdapter;

public record DeviceInfo(long id,

                         @JsonSerialize(using = ByteArrayBase64WithPaddingAdapter.Serializing.class)
                         @JsonDeserialize(using = ByteArrayBase64WithPaddingAdapter.Deserializing.class)
                         byte[] name,

                         long lastSeen,
                         long created) {

  public static DeviceInfo forDevice(final Device device) {
    return new DeviceInfo(device.getId(), device.getName(), device.getLastSeen(), device.getCreated());
  }
}
