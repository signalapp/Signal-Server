/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.security.InvalidKeyException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.signal.libsignal.zkgroup.auth.ServerZkAuthOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.entities.DeliveryCertificate;
import org.whispersystems.textsecuregcm.entities.GroupCredentials;
import org.whispersystems.textsecuregcm.util.Util;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/certificate")
@Tag(name = "Certificate")
public class CertificateController {

  private final CertificateGenerator certificateGenerator;
  private final ServerZkAuthOperations serverZkAuthOperations;
  private final Clock clock;

  @VisibleForTesting
  public static final Duration MAX_REDEMPTION_DURATION = Duration.ofDays(7);
  private static final String GENERATE_DELIVERY_CERTIFICATE_COUNTER_NAME = name(CertificateGenerator.class, "generateCertificate");
  private static final String INCLUDE_E164_TAG_NAME = "includeE164";

  public CertificateController(
      @Nonnull CertificateGenerator certificateGenerator,
      @Nonnull ServerZkAuthOperations serverZkAuthOperations,
      @Nonnull Clock clock) {
    this.certificateGenerator = Objects.requireNonNull(certificateGenerator);
    this.serverZkAuthOperations = Objects.requireNonNull(serverZkAuthOperations);
    this.clock = Objects.requireNonNull(clock);
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/delivery")
  public DeliveryCertificate getDeliveryCertificate(@Auth AuthenticatedAccount auth,
      @QueryParam("includeE164") @DefaultValue("true") boolean includeE164)
      throws InvalidKeyException {

    if (Util.isEmpty(auth.getAccount().getIdentityKey())) {
      throw new WebApplicationException(Response.Status.BAD_REQUEST);
    }

    Metrics.counter(GENERATE_DELIVERY_CERTIFICATE_COUNTER_NAME, INCLUDE_E164_TAG_NAME, String.valueOf(includeE164))
        .increment();

    return new DeliveryCertificate(
        certificateGenerator.createFor(auth.getAccount(), auth.getAuthenticatedDevice(), includeE164));
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/auth/group")
  public GroupCredentials getGroupAuthenticationCredentials(
      @Auth AuthenticatedAccount auth,
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

    UUID aci = auth.getAccount().getUuid();
    UUID pni = auth.getAccount().getPhoneNumberIdentifier();
    while (!redemption.isAfter(redemptionEnd)) {
      credentials.add(new GroupCredentials.GroupCredential(
          serverZkAuthOperations.issueAuthCredentialWithPni(aci, pni, redemption).serialize(),
          (int) redemption.getEpochSecond()));

      redemption = redemption.plus(Duration.ofDays(1));
    }

    return new GroupCredentials(credentials, pni);
  }
}
