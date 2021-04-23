/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import java.util.UUID;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import org.whispersystems.textsecuregcm.providers.MultiRecipientMessageProvider;

public class MultiRecipientMessage {

  public static class Recipient {

    @NotNull
    private final UUID uuid;

    @Min(1)
    private final long deviceId;

    @Size(min = 48, max = 48)
    @NotNull
    private final byte[] perRecipientKeyMaterial;

    public Recipient(UUID uuid, long deviceId, byte[] perRecipientKeyMaterial) {
      this.uuid = uuid;
      this.deviceId = deviceId;
      this.perRecipientKeyMaterial = perRecipientKeyMaterial;
    }

    public UUID getUuid() {
      return uuid;
    }

    public long getDeviceId() {
      return deviceId;
    }

    public byte[] getPerRecipientKeyMaterial() {
      return perRecipientKeyMaterial;
    }
  }

  @NotNull
  @Size(min = 1, max = MultiRecipientMessageProvider.MAX_RECIPIENT_COUNT)
  private final Recipient[] recipients;

  @NotNull
  @Min(32)
  private final byte[] commonPayload;

  public MultiRecipientMessage(Recipient[] recipients, byte[] commonPayload) {
    this.recipients = recipients;
    this.commonPayload = commonPayload;
  }

  public Recipient[] getRecipients() {
    return recipients;
  }

  public byte[] getCommonPayload() {
    return commonPayload;
  }
}
