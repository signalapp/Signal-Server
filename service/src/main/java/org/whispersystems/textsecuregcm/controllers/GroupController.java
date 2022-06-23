/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.Auth;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.signal.libsignal.zkgroup.auth.ServerZkAuthOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.entities.GroupCredentials;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

@Path("/v1/group")
public class GroupController {

  private final ServerZkAuthOperations serverZkAuthOperations;
  private final Clock clock;

  @VisibleForTesting
  static final Duration MAX_REDEMPTION_DURATION = Duration.ofDays(7);

  public GroupController(final ServerZkAuthOperations serverZkAuthOperations) {
    this(serverZkAuthOperations, Clock.systemUTC());
  }

  @VisibleForTesting
  GroupController(final ServerZkAuthOperations serverZkAuthOperations, final Clock clock) {
    this.serverZkAuthOperations = serverZkAuthOperations;
    this.clock = clock;
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/auth")
  public GroupCredentials getGroupAuthenticationCredentials(@Auth AuthenticatedAccount auth,
      @QueryParam("redemptionStartSeconds") int startSeconds,
      @QueryParam("redemptionEndSeconds") int endSeconds) {

    final Instant startOfDay = clock.instant().truncatedTo(ChronoUnit.DAYS);
    final Instant redemptionStart = Instant.ofEpochSecond(startSeconds);
    final Instant redemptionEnd = Instant.ofEpochSecond(endSeconds);

    if (redemptionStart.isAfter(redemptionEnd) ||
        redemptionStart.isBefore(startOfDay) ||
        redemptionEnd.isAfter(startOfDay.plus(MAX_REDEMPTION_DURATION)) ||
        !redemptionStart.equals(redemptionStart.truncatedTo(ChronoUnit.DAYS)) ||
        !redemptionEnd.equals(redemptionEnd.truncatedTo(ChronoUnit.DAYS))) {

      throw new BadRequestException();
    }

    final List<GroupCredentials.GroupCredential> credentials = new ArrayList<>();

    Instant redemption = redemptionStart;

    while (!redemption.isAfter(redemptionEnd)) {
      credentials.add(new GroupCredentials.GroupCredential(serverZkAuthOperations.issueAuthCredentialWithPni(
          auth.getAccount().getUuid(),
          auth.getAccount().getPhoneNumberIdentifier(),
          redemption).serialize(),
          (int) redemption.getEpochSecond()));

      redemption = redemption.plus(Duration.ofDays(1));
    }

    return new GroupCredentials(credentials);
  }
}
