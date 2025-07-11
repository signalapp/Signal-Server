/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.ChangesPhoneNumber;
import org.whispersystems.textsecuregcm.auth.PhoneVerificationTokenManager;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.entities.AccountDataReportResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ChangeNumberRequest;
import org.whispersystems.textsecuregcm.entities.MismatchedDevicesResponse;
import org.whispersystems.textsecuregcm.entities.PhoneNumberDiscoverabilityRequest;
import org.whispersystems.textsecuregcm.entities.PhoneNumberIdentityKeyDistributionRequest;
import org.whispersystems.textsecuregcm.entities.PhoneVerificationRequest;
import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.entities.StaleDevicesResponse;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.MessageTooLargeException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberManager;
import org.whispersystems.textsecuregcm.storage.Device;

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

  public AccountControllerV2(final AccountsManager accountsManager,
      final ChangeNumberManager changeNumberManager,
      final PhoneVerificationTokenManager phoneVerificationTokenManager,
      final RegistrationLockVerificationManager registrationLockVerificationManager,
      final RateLimiters rateLimiters) {

    this.accountsManager = accountsManager;
    this.changeNumberManager = changeNumberManager;
    this.phoneVerificationTokenManager = phoneVerificationTokenManager;
    this.registrationLockVerificationManager = registrationLockVerificationManager;
    this.rateLimiters = rateLimiters;
  }

  @PUT
  @Path("/number")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ChangesPhoneNumber
  @Operation(summary = "Change number", description = "Changes a phone number for an existing account.")
  @ApiResponse(responseCode = "200", description = "The phone number associated with the authenticated account was changed successfully", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  @ApiResponse(responseCode = "403", description = "Verification failed for the provided Registration Recovery Password")
  @ApiResponse(responseCode = "409", description = "Mismatched number of devices or device ids in 'devices to notify' list", content = @Content(schema = @Schema(implementation = MismatchedDevicesResponse.class)))
  @ApiResponse(responseCode = "410", description = "Mismatched registration ids in 'devices to notify' list", content = @Content(schema = @Schema(implementation = StaleDevicesResponse.class)))
  @ApiResponse(responseCode = "413", description = "One or more device messages was too large")
  @ApiResponse(responseCode = "422", description = "The request did not pass validation")
  @ApiResponse(responseCode = "423", content = @Content(schema = @Schema(implementation = RegistrationLockFailure.class)))
  @ApiResponse(responseCode = "429", description = "Too many attempts", headers = @Header(
      name = "Retry-After",
      description = "If present, an positive integer indicating the number of seconds before a subsequent attempt could succeed"))
  public AccountIdentityResponse changeNumber(@Auth final AuthenticatedDevice authenticatedDevice,
      @NotNull @Valid final ChangeNumberRequest request,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgentString,
      @Context final ContainerRequestContext requestContext) throws RateLimitExceededException, InterruptedException {

    if (authenticatedDevice.deviceId() != Device.PRIMARY_ID) {
      throw new ForbiddenException();
    }

    if (!request.isSignatureValidOnEachSignedPreKey(userAgentString)) {
      throw new WebApplicationException("Invalid signature", 422);
    }

    final String number = request.number();

    final Account account = accountsManager.getByAccountIdentifier(authenticatedDevice.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

    // Only verify and check reglock if there's a data change to be made...
    if (!account.getNumber().equals(number)) {

      rateLimiters.getRegistrationLimiter().validate(number);

      final PhoneVerificationRequest.VerificationType verificationType = phoneVerificationTokenManager.verify(
          requestContext, number, request);

      final Optional<Account> existingAccount = accountsManager.getByE164(number);

      if (existingAccount.isPresent()) {
        registrationLockVerificationManager.verifyRegistrationLock(existingAccount.get(), request.registrationLock(),
            userAgentString, RegistrationLockVerificationManager.Flow.CHANGE_NUMBER, verificationType);
      }

      Metrics.counter(CHANGE_NUMBER_COUNTER_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgentString),
              Tag.of(VERIFICATION_TYPE_TAG_NAME, verificationType.name())))
          .increment();
    }

    // ...but always attempt to make the change in case a client retries and needs to re-send messages
    try {
      final Account updatedAccount = changeNumberManager.changeNumber(
          account,
          request.number(),
          request.pniIdentityKey(),
          request.devicePniSignedPrekeys(),
          request.devicePniPqLastResortPrekeys(),
          request.deviceMessages(),
          request.pniRegistrationIds(),
          userAgentString);

      return AccountIdentityResponseBuilder.fromAccount(updatedAccount);
    } catch (MismatchedDevicesException e) {
      if (!e.getMismatchedDevices().staleDeviceIds().isEmpty()) {
        throw new WebApplicationException(Response.status(410)
            .type(MediaType.APPLICATION_JSON)
            .entity(new StaleDevicesResponse(e.getMismatchedDevices().staleDeviceIds()))
            .build());
      } else {
        throw new WebApplicationException(Response.status(409)
            .type(MediaType.APPLICATION_JSON_TYPE)
            .entity(new MismatchedDevicesResponse(e.getMismatchedDevices().missingDeviceIds(),
                e.getMismatchedDevices().extraDeviceIds()))
            .build());
      }
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    } catch (MessageTooLargeException e) {
      throw new WebApplicationException(Response.Status.REQUEST_ENTITY_TOO_LARGE);
    }
  }

  // TODO Remove entirely on or after 2025-10-08
  @Deprecated(forRemoval = true)
  @PUT
  @Path("/phone_number_identity_key_distribution")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Set phone-number identity keys",
      description = "Updates key material for the phone-number identity for all devices and sends a synchronization message to companion devices")
  @ApiResponse(responseCode = "200", description = "Indicates the transaction was successful and returns basic information about this account.", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "401", description = "Account authentication check failed.")
  @ApiResponse(responseCode = "403", description = "This endpoint can only be invoked from the account's primary device.")
  @ApiResponse(responseCode = "422", description = "The request body failed validation.")
  public AccountIdentityResponse distributePhoneNumberIdentityKeys(
      @Auth final AuthenticatedDevice authenticatedDevice,
      @HeaderParam(HttpHeaders.USER_AGENT) @Nullable final String userAgentString,
      @NotNull @Valid final PhoneNumberIdentityKeyDistributionRequest request) {

    if (authenticatedDevice.deviceId() != Device.PRIMARY_ID) {
      throw new ForbiddenException();
    }

    if (!request.isSignatureValidOnEachSignedPreKey(userAgentString)) {
      throw new WebApplicationException("Invalid signature", 422);
    }

    return AccountIdentityResponseBuilder.fromAccount(accountsManager.getByAccountIdentifier(authenticatedDevice.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED)));
  }

  @PUT
  @Path("/phone_number_discoverability")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Sets whether the account should be discoverable by phone number in the directory.")
  @ApiResponse(responseCode = "204", description = "The setting was successfully updated.")
  public void setPhoneNumberDiscoverability(
      @Auth AuthenticatedDevice auth,
      @NotNull @Valid PhoneNumberDiscoverabilityRequest phoneNumberDiscoverability) {

    final Account account = accountsManager.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

    accountsManager.update(account, a -> a.setDiscoverableByPhoneNumber(
        phoneNumberDiscoverability.discoverableByPhoneNumber()));
  }

  @GET
  @Path("/data_report")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Produces a report of non-ephemeral account data stored by the service")
  @ApiResponse(responseCode = "200",
      description = "Response with data report. A plain text representation is a field in the response.",
      useReturnTypeSchema = true)
  public AccountDataReportResponse getAccountDataReport(@Auth final AuthenticatedDevice auth) {

    final Account account = accountsManager.getByAccountIdentifier(auth.accountIdentifier())
        .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

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
