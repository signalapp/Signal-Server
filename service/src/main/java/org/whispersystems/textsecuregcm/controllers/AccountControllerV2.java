/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
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
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.ChangesPhoneNumber;
import org.whispersystems.textsecuregcm.entities.AccountDataReportResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ChangeNumberRequest;
import org.whispersystems.textsecuregcm.entities.MismatchedDevicesResponse;
import org.whispersystems.textsecuregcm.entities.PhoneNumberDiscoverabilityRequest;
import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.entities.StaleDevicesResponse;
import org.whispersystems.textsecuregcm.push.MessageTooLargeException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberManager;
import org.whispersystems.textsecuregcm.storage.Device;

@Path("/v2/accounts")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Account")
public class AccountControllerV2 {

  private final AccountsManager accountsManager;
  private final ChangeNumberManager changeNumberManager;

  public AccountControllerV2(final AccountsManager accountsManager,
      final ChangeNumberManager changeNumberManager) {

    this.accountsManager = accountsManager;
    this.changeNumberManager = changeNumberManager;
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

    try {
      final Account updatedAccount = changeNumberManager.changeNumber(
          authenticatedDevice.accountIdentifier(),
          StringUtils.isNotBlank(request.sessionId()) ? request.decodeSessionId() : null,
          request.recoveryPassword(),
          request.registrationLock(),
          request.number(),
          request.pniIdentityKey(),
          request.devicePniSignedPrekeys(),
          request.devicePniPqLastResortPrekeys(),
          request.deviceMessages(),
          request.pniRegistrationIds(),
          requestContext);

      return AccountIdentityResponseBuilder.fromAccount(updatedAccount);
    } catch (final MismatchedDevicesException e) {
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
    } catch (final IllegalArgumentException e) {
      throw new BadRequestException(e);
    } catch (final MessageTooLargeException e) {
      throw new WebApplicationException(Response.Status.REQUEST_ENTITY_TOO_LARGE);
    }
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

    accountsManager.update(auth.accountIdentifier(), a -> a.setDiscoverableByPhoneNumber(
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
