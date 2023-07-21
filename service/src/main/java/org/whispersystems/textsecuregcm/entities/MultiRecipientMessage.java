/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.Arrays;
import java.util.Objects;
import javax.validation.Valid;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.providers.MultiRecipientMessageProvider;
import org.whispersystems.textsecuregcm.util.Pair;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;

public record MultiRecipientMessage(
    @NotNull @Size(min = 1, max = MultiRecipientMessageProvider.MAX_RECIPIENT_COUNT) @Valid Recipient[] recipients,
    @NotNull @Size(min = 32) byte[] commonPayload) {

  private static final Counter REJECT_DUPLICATE_RECIPIENT_COUNTER =
      Metrics.counter(
          name(MessageController.class, "rejectDuplicateRecipients"),
          "multiRecipient", "false");

  public record Recipient(@NotNull
                          @JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
                          @JsonDeserialize(using = ServiceIdentifierAdapter.ServiceIdentifierDeserializer.class)
                          ServiceIdentifier uuid,
                          @Min(1) long deviceId,
                          @Min(0) @Max(65535) int registrationId,
                          @Size(min = 48, max = 48) @NotNull byte[] perRecipientKeyMaterial) {

    @Override
    public boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      Recipient recipient = (Recipient) o;
      return deviceId == recipient.deviceId && registrationId == recipient.registrationId && uuid.equals(recipient.uuid)
          && Arrays.equals(perRecipientKeyMaterial, recipient.perRecipientKeyMaterial);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(uuid, deviceId, registrationId);
      result = 31 * result + Arrays.hashCode(perRecipientKeyMaterial);
      return result;
    }
  }

  public MultiRecipientMessage(Recipient[] recipients, byte[] commonPayload) {
    this.recipients = recipients;
    this.commonPayload = commonPayload;
  }

  @AssertTrue
  public boolean hasNoDuplicateRecipients() {
    boolean valid =
        Arrays.stream(recipients).map(r -> new Pair<>(r.uuid(), r.deviceId())).distinct().count() == recipients.length;
    if (!valid) {
      REJECT_DUPLICATE_RECIPIENT_COUNTER.increment();
    }
    return valid;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    MultiRecipientMessage that = (MultiRecipientMessage) o;
    return Arrays.equals(recipients, that.recipients) && Arrays.equals(commonPayload, that.commonPayload);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(recipients);
    result = 31 * result + Arrays.hashCode(commonPayload);
    return result;
  }
}
