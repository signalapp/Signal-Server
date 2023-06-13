/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.Arrays;
import java.util.UUID;
import javax.validation.Valid;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.providers.MultiRecipientMessageProvider;
import org.whispersystems.textsecuregcm.util.Pair;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;

public class MultiRecipientMessage {

  private static final Counter REJECT_DUPLICATE_RECIPIENT_COUNTER =
      Metrics.counter(
          name(MessageController.class, "rejectDuplicateRecipients"),
          "multiRecipient", "false");

  public static class Recipient {

    @NotNull
    private final UUID uuid;

    @Min(1)
    private final long deviceId;

    @Min(0)
    @Max(65535)
    private final int registrationId;

    @Size(min = 48, max = 48)
    @NotNull
    private final byte[] perRecipientKeyMaterial;

    public Recipient(UUID uuid, long deviceId, int registrationId, byte[] perRecipientKeyMaterial) {
      this.uuid = uuid;
      this.deviceId = deviceId;
      this.registrationId = registrationId;
      this.perRecipientKeyMaterial = perRecipientKeyMaterial;
    }

    public UUID getUuid() {
      return uuid;
    }

    public long getDeviceId() {
      return deviceId;
    }

    public int getRegistrationId() {
      return registrationId;
    }

    public byte[] getPerRecipientKeyMaterial() {
      return perRecipientKeyMaterial;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      Recipient recipient = (Recipient) o;

      if (deviceId != recipient.deviceId)
        return false;
      if (registrationId != recipient.registrationId)
        return false;
      if (!uuid.equals(recipient.uuid))
        return false;
      return Arrays.equals(perRecipientKeyMaterial, recipient.perRecipientKeyMaterial);
    }

    @Override
    public int hashCode() {
      int result = uuid.hashCode();
      result = 31 * result + (int) (deviceId ^ (deviceId >>> 32));
      result = 31 * result + registrationId;
      result = 31 * result + Arrays.hashCode(perRecipientKeyMaterial);
      return result;
    }

    public String toString() {
      return "Recipient(" + uuid + ", " + deviceId + ", " + registrationId + ", " + Arrays.toString(perRecipientKeyMaterial) + ")";
    }
  }

  @NotNull
  @Size(min = 1, max = MultiRecipientMessageProvider.MAX_RECIPIENT_COUNT)
  @Valid
  private final Recipient[] recipients;

  @NotNull
  @Size(min = 32)
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

  @AssertTrue
  public boolean hasNoDuplicateRecipients() {
    boolean valid = Arrays.stream(recipients).map(r -> new Pair<>(r.getUuid(), r.getDeviceId())).distinct().count() == recipients.length;
    if (!valid) {
      REJECT_DUPLICATE_RECIPIENT_COUNTER.increment();
    }
    return valid;
  }
}
