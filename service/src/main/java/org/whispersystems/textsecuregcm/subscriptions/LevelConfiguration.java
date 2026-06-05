package org.whispersystems.textsecuregcm.subscriptions;

import io.swagger.v3.oas.annotations.media.Schema;
import org.whispersystems.textsecuregcm.entities.Badge;

@Schema(description = "Configuration for a donation level - use to present appropriate client interfaces")
public record LevelConfiguration(
    @Schema(description = "The displayable badge associated with the level")
    Badge badge) {}
