/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.codahale.metrics.annotation.Timed;
import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.PhoneVerificationTokenManager;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.entities.AccountDataReportResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ChangeNumberRequest;
import org.whispersystems.textsecuregcm.entities.MismatchedDevices;
import org.whispersystems.textsecuregcm.entities.PhoneNumberDiscoverabilityRequest;
import org.whispersystems.textsecuregcm.entities.PhoneVerificationRequest;
import org.whispersystems.textsecuregcm.entities.PhoneNumberIdentityKeyDistributionRequest;
import org.whispersystems.textsecuregcm.entities.StaleDevices;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberManager;

@Path("/v2/accounts")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Account")
public class AccountControllerV2 {

  private static final String CHANGE_NUMBER_COUNTER_NAME = name(AccountControllerV2.class, "changeNumber");
  private static final String VERIFICATION_TYPE_TAG_NAME = "verification";

  private final AccountsManager accountsManager;
  private final ChangeNumberManager changeNumberManager;
  private final PhoneVerificationTokenManager phoneVerificationTokenManager;
  private final RegistrationLockVerificationManager registrationLockVerificationManager;
  private final RateLimiters rateLimiters;

  public AccountControllerV2(final AccountsManager accountsManager, final ChangeNumberManager changeNumberManager,
      final PhoneVerificationTokenManager phoneVerificationTokenManager,
      final RegistrationLockVerificationManager registrationLockVerificationManager, final RateLimiters rateLimiters) {
    this.accountsManager = accountsManager;
    this.changeNumberManager = changeNumberManager;
    this.phoneVerificationTokenManager = phoneVerificationTokenManager;
    this.registrationLockVerificationManager = registrationLockVerificationManager;
    this.rateLimiters = rateLimiters;
  }

  @Timed
  @PUT
  @Path("/number")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public AccountIdentityResponse changeNumber(@Auth final AuthenticatedAccount authenticatedAccount,
      @NotNull @Valid final ChangeNumberRequest request, @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent)
      throws RateLimitExceededException, InterruptedException {

    if (!authenticatedAccount.getAuthenticatedDevice().isMaster()) {
      throw new ForbiddenException();
    }

    final String number = request.number();

    // Only verify and check reglock if there's a data change to be made...
    if (!authenticatedAccount.getAccount().getNumber().equals(number)) {

      RateLimiter.adaptLegacyException(() -> rateLimiters.getRegistrationLimiter().validate(number));

      final PhoneVerificationRequest.VerificationType verificationType = phoneVerificationTokenManager.verify(number,
          request);

      final Optional<Account> existingAccount = accountsManager.getByE164(number);

      if (existingAccount.isPresent()) {
        registrationLockVerificationManager.verifyRegistrationLock(existingAccount.get(), request.registrationLock(),
            userAgent, RegistrationLockVerificationManager.Flow.CHANGE_NUMBER, verificationType);
      }

      Metrics.counter(CHANGE_NUMBER_COUNTER_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent),
              Tag.of(VERIFICATION_TYPE_TAG_NAME, verificationType.name())))
          .increment();
    }

    // ...but always attempt to make the change in case a client retries and needs to re-send messages
    try {
      final Account updatedAccount = changeNumberManager.changeNumber(
          authenticatedAccount.getAccount(),
          request.number(),
          request.pniIdentityKey(),
          request.devicePniSignedPrekeys(),
          request.deviceMessages(),
          request.pniRegistrationIds());

      return new AccountIdentityResponse(
          updatedAccount.getUuid(),
          updatedAccount.getNumber(),
          updatedAccount.getPhoneNumberIdentifier(),
          updatedAccount.getUsernameHash().orElse(null),
          updatedAccount.isStorageSupported());
    } catch (MismatchedDevicesException e) {
      throw new WebApplicationException(Response.status(409)
          .type(MediaType.APPLICATION_JSON_TYPE)
          .entity(new MismatchedDevices(e.getMissingDevices(),
              e.getExtraDevices()))
          .build());
    } catch (StaleDevicesException e) {
      throw new WebApplicationException(Response.status(410)
          .type(MediaType.APPLICATION_JSON)
          .entity(new StaleDevices(e.getStaleDevices()))
          .build());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    }
  }

  @Timed
  @PUT
  @Path("/phone_number_identity_key_distribution")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Updates key material for the phone-number identity for all devices and sends a synchronization message to companion devices")
  public AccountIdentityResponse distributePhoneNumberIdentityKeys(@Auth final AuthenticatedAccount authenticatedAccount,
      @NotNull @Valid final PhoneNumberIdentityKeyDistributionRequest request) {

    if (!authenticatedAccount.getAuthenticatedDevice().isMaster()) {
      throw new ForbiddenException();
    }

    final Account account = authenticatedAccount.getAccount();
    if (!account.isPniSupported()) {
      throw new WebApplicationException(Response.status(425).build());
    }

    try {
      final Account updatedAccount = changeNumberManager.updatePNIKeys(
          authenticatedAccount.getAccount(),
          request.pniIdentityKey(),
          request.devicePniSignedPrekeys(),
          request.deviceMessages(),
          request.pniRegistrationIds());

      return new AccountIdentityResponse(
          updatedAccount.getUuid(),
          updatedAccount.getNumber(),
          updatedAccount.getPhoneNumberIdentifier(),
          updatedAccount.getUsernameHash().orElse(null),
          updatedAccount.isStorageSupported());
    } catch (MismatchedDevicesException e) {
      throw new WebApplicationException(Response.status(409)
          .type(MediaType.APPLICATION_JSON_TYPE)
          .entity(new MismatchedDevices(e.getMissingDevices(),
                  e.getExtraDevices()))
          .build());
    } catch (StaleDevicesException e) {
      throw new WebApplicationException(Response.status(410)
          .type(MediaType.APPLICATION_JSON)
          .entity(new StaleDevices(e.getStaleDevices()))
          .build());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    }
  }

  @Timed
  @PUT
  @Path("/phone_number_discoverability")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void setPhoneNumberDiscoverability(
      @Auth AuthenticatedAccount auth,
      @NotNull @Valid PhoneNumberDiscoverabilityRequest phoneNumberDiscoverability
  ) {
    accountsManager.update(auth.getAccount(), a -> a.setDiscoverableByPhoneNumber(
        phoneNumberDiscoverability.discoverableByPhoneNumber()));
  }

  @Timed
  @GET
  @Path("/data_report")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Produces a report of non-ephemeral account data stored by the service")
  @ApiResponse(responseCode = "200",
      description = "Response with data report. A plain text representation is a field in the response.",
      useReturnTypeSchema = true)
  public AccountDataReportResponse getAccountDataReport(@Auth final AuthenticatedAccount auth) {

    final Account account = auth.getAccount();

    return new AccountDataReportResponse(UUID.randomUUID(), Instant.now(),
        new AccountDataReportResponse.AccountAndDevicesDataReport(
            new AccountDataReportResponse.AccountDataReport(
                account.getNumber(),
                account.getBadges().stream().map(AccountDataReportResponse.BadgeDataReport::new).toList(),
                account.isUnrestrictedUnidentifiedAccess(),
                account.isDiscoverableByPhoneNumber()),
            account.getDevices().stream().map(device ->
                new AccountDataReportResponse.DeviceDataReport(
                    device.getId(),
                    Instant.ofEpochMilli(device.getLastSeen()),
                    Instant.ofEpochMilli(device.getCreated()),
                    device.getUserAgent())).toList()));
  }
}
