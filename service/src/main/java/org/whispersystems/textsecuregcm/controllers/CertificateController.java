/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.security.InvalidKeyException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.auth.AuthCredentialWithPniResponse;
import org.signal.libsignal.zkgroup.auth.ServerZkAuthOperations;
import org.signal.libsignal.zkgroup.calllinks.CallLinkAuthCredentialResponse;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.entities.DeliveryCertificate;
import org.whispersystems.textsecuregcm.entities.GroupCredentials;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/certificate")
@Tag(name = "Certificate")
public class CertificateController {

  private final AccountsManager accountsManager;
  private final CertificateGenerator certificateGenerator;
  private final ServerZkAuthOperations serverZkAuthOperations;
  private final GenericServerSecretParams genericServerSecretParams;
  private final Clock clock;

  @VisibleForTesting
  public static final Duration MAX_REDEMPTION_DURATION = Duration.ofDays(7);
  private static final String GENERATE_DELIVERY_CERTIFICATE_COUNTER_NAME = name(CertificateGenerator.class, "generateCertificate");
  private static final String INCLUDE_E164_TAG_NAME = "includeE164";

  public CertificateController(
      final AccountsManager accountsManager,
      @Nonnull CertificateGenerator certificateGenerator,
      @Nonnull ServerZkAuthOperations serverZkAuthOperations,
      @Nonnull GenericServerSecretParams genericServerSecretParams,
      @Nonnull Clock clock) {

    this.accountsManager = accountsManager;
    this.certificateGenerator = Objects.requireNonNull(certificateGenerator);
    this.serverZkAuthOperations = Objects.requireNonNull(serverZkAuthOperations);
    this.genericServerSecretParams = genericServerSecretParams;
    this.clock = Objects.requireNonNull(clock);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/delivery")
  public DeliveryCertificate getDeliveryCertificate(@Auth AuthenticatedDevice auth,
      @QueryParam("includeE164") @DefaultValue("true") boolean includeE164)
      throws InvalidKeyException {

    Metrics.counter(GENERATE_DELIVERY_CERTIFICATE_COUNTER_NAME, INCLUDE_E164_TAG_NAME, String.valueOf(includeE164))
        .increment();

    final Account account = accountsManager.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

    return new DeliveryCertificate(
        certificateGenerator.createFor(account, auth.deviceId(), includeE164));
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/auth/group")
  public GroupCredentials getGroupAuthenticationCredentials(
      @Auth AuthenticatedDevice auth,
      @QueryParam("redemptionStartSeconds") long startSeconds,
      @QueryParam("redemptionEndSeconds") long endSeconds) {

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

    final Account account = accountsManager.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

    final List<GroupCredentials.GroupCredential> credentials = new ArrayList<>();
    final List<GroupCredentials.CallLinkAuthCredential> callLinkAuthCredentials = new ArrayList<>();

    Instant redemption = redemptionStart;

    final ServiceId.Aci aci = new ServiceId.Aci(account.getIdentifier(IdentityType.ACI));
    final ServiceId.Pni pni = new ServiceId.Pni(account.getIdentifier(IdentityType.PNI));

    while (!redemption.isAfter(redemptionEnd)) {
      AuthCredentialWithPniResponse authCredentialWithPni = serverZkAuthOperations.issueAuthCredentialWithPniZkc(aci, pni, redemption);
      credentials.add(new GroupCredentials.GroupCredential(
          authCredentialWithPni.serialize(),
          (int) redemption.getEpochSecond()));

      callLinkAuthCredentials.add(new GroupCredentials.CallLinkAuthCredential(
          CallLinkAuthCredentialResponse.issueCredential(aci, redemption, genericServerSecretParams).serialize(),
          redemption.getEpochSecond()));

      redemption = redemption.plus(Duration.ofDays(1));
    }

    return new GroupCredentials(credentials, callLinkAuthCredentials, pni.getRawUUID());
  }
}
