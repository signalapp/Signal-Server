/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import org.signal.chat.common.Badge;
import org.signal.chat.common.BadgeSvg;

public class BadgeGrpcHelper {

  public static Badge toGrpcBadge(final org.whispersystems.textsecuregcm.entities.Badge badge) {
    final Badge.Builder builder = Badge.newBuilder()
        .setId(badge.getId())
        .setCategory(badge.getCategory())
        .setName(badge.getName())
        .setDescription(badge.getDescription())
        .addAllSprites6(badge.getSprites6())
        .setSvg(badge.getSvg());

    badge.getSvgs().stream()
        .map(badgeSvg -> BadgeSvg.newBuilder()
            .setDark(badgeSvg.getDark())
            .setLight(badgeSvg.getLight())
            .build())
        .forEach(builder::addSvgs);

    return builder.build();
  }

}
