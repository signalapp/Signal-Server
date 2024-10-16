/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.storage.AccountBadge;

public record AccountDataReportResponse(UUID reportId,
                                        @JsonSerialize(using = InstantSerializer.class)
                                        @JsonFormat(pattern = DATE_FORMAT, timezone = UTC)
                                        Instant reportTimestamp,
                                        AccountAndDevicesDataReport data) {

  private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
  private static final String UTC = "Etc/UTC";

  @JsonProperty
  @Schema(description = "A plaintext representation of the data report")
  String text() {

    final StringBuilder builder = new StringBuilder();

    // header
    builder.append(String.format("""
            Report ID: %s
            Report timestamp: %s
                          
            """,
        reportId,
        reportTimestamp.truncatedTo(ChronoUnit.SECONDS)));

    // account
    builder.append(String.format("""
            # Account
            Phone number: %s
            Allow sealed sender from anyone: %s
            Find account by phone number: %s
            """,
        data.account.phoneNumber(),
        data.account.allowSealedSenderFromAnyone(),
        data.account.findAccountByPhoneNumber()));

    // badges
    builder.append("Badges:");

    if (data.account.badges().isEmpty()) {
      builder.append(" None\n");
    } else {
      builder.append("\n");
      data.account.badges().forEach(badgeDataReport -> builder.append(String.format("""
              - ID: %s
                Expiration: %s
                Visible: %s
              """,
          badgeDataReport.id(),
          badgeDataReport.expiration().truncatedTo(ChronoUnit.SECONDS),
          badgeDataReport.visible())));
    }

    // devices
    builder.append("\n# Devices\n");

    data.devices().forEach(deviceDataReport ->
        builder.append(String.format("""
                - ID: %s
                  Created: %s
                  Last seen: %s
                  User-agent: %s
                """,
            deviceDataReport.id(),
            deviceDataReport.created().truncatedTo(ChronoUnit.SECONDS),
            deviceDataReport.lastSeen().truncatedTo(ChronoUnit.SECONDS),
            deviceDataReport.userAgent())));

    return builder.toString();
  }


  public record AccountAndDevicesDataReport(AccountDataReport account,
                                            List<DeviceDataReport> devices) {

  }

  public record AccountDataReport(String phoneNumber, List<BadgeDataReport> badges, boolean allowSealedSenderFromAnyone,
                                  boolean findAccountByPhoneNumber) {

  }

  public record DeviceDataReport(byte id,
                                 @JsonFormat(pattern = DATE_FORMAT, timezone = UTC)
                                 Instant lastSeen,
                                 @JsonFormat(pattern = DATE_FORMAT, timezone = UTC)
                                 Instant created,
                                 @Nullable String userAgent) {


  }

  public record BadgeDataReport(String id,
                                @JsonFormat(pattern = DATE_FORMAT, timezone = UTC)
                                Instant expiration,
                                boolean visible) {

    public BadgeDataReport(AccountBadge badge) {
      this(badge.id(), badge.expiration(), badge.visible());
    }

  }

}
