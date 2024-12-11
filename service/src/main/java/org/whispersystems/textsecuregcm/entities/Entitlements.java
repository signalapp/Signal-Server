package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.v3.oas.annotations.media.Schema;
import org.whispersystems.textsecuregcm.util.InstantAdapter;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.List;

public record Entitlements(
    @Schema(description = "Active badges added via /v1/donation/redeem-receipt")
    List<BadgeEntitlement> badges,
    @Schema(description = "If present, the backup level set via /v1/archives/redeem-receipt")
    @Nullable BackupEntitlement backup) {

  public record BadgeEntitlement(
      @Schema(description = "The badge id")
      String id,

      @Schema(description = "When the badge expires, in number of seconds since epoch", implementation = Long.class)
      @JsonSerialize(using = InstantAdapter.EpochSecondSerializer.class)
      @JsonFormat(shape = JsonFormat.Shape.NUMBER_INT)
      @JsonProperty("expirationSeconds")
      Instant expiration,

      @Schema(description = "Whether the badge is currently configured to be visible")
      boolean visible) {
  }

  public record BackupEntitlement(
      @Schema(description = "The backup level of the account")
      long backupLevel,

      @Schema(description = "When the backup entitlement expires, in number of seconds since epoch", implementation = Long.class)
      @JsonSerialize(using = InstantAdapter.EpochSecondSerializer.class)
      @JsonFormat(shape = JsonFormat.Shape.NUMBER_INT)
      @JsonProperty("expirationSeconds")
      Instant expiration) {
  }
}
