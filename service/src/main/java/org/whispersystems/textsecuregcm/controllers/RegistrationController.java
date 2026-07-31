/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.net.HttpHeaders;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.BasicAuthorizationHeader;
import org.whispersystems.textsecuregcm.auth.InvalidRegistrationSessionException;
import org.whispersystems.textsecuregcm.auth.PhoneVerificationTokenManager;
import org.whispersystems.textsecuregcm.auth.RecoveryPasswordVerificationFailedException;
import org.whispersystems.textsecuregcm.auth.RegistrationLockFailureException;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.auth.UnverifiedRegistrationSessionException;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountCreationResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.PhoneVerificationRequest;
import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.entities.RegistrationRequest;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.spam.RegistrationFraudChecker;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.DeviceIdentityInfo;
import org.whispersystems.textsecuregcm.storage.DeviceSpec;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.ReceiptAlreadyRedeemedException;
import org.whispersystems.textsecuregcm.subscriptions.ReceiptCredentialPresentationFactory;
import org.whispersystems.textsecuregcm.subscriptions.ReceiptLevel;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.Util;

@Path("/v1/registration")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Registration")
public class RegistrationController {

  private static final DistributionSummary REREGISTRATION_IDLE_DAYS_DISTRIBUTION = DistributionSummary
      .builder(name(RegistrationController.class, "reregistrationIdleDays"))
      .distributionStatisticExpiry(Duration.ofHours(2))
      .register(Metrics.globalRegistry);

  private static final String ACCOUNT_CREATED_COUNTER_NAME = name(RegistrationController.class, "accountCreated");
  private static final String COUNTRY_CODE_TAG_NAME = "countryCode";
  private static final String REGION_CODE_TAG_NAME = "regionCode";
  private static final String VERIFICATION_TYPE_TAG_NAME = "verification";

  private final AccountsManager accounts;
  private final PhoneVerificationTokenManager phoneVerificationTokenManager;
  private final RegistrationLockVerificationManager registrationLockVerificationManager;
  private final RateLimiters rateLimiters;
  private final RegistrationFraudChecker registrationFraudChecker;
  private final ReceiptCredentialPresentationFactory receiptCredentialPresentationFactory;
  private final ServerZkReceiptOperations serverZkReceiptOperations;
  private final Clock clock;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  private static final Logger logger = LoggerFactory.getLogger(RegistrationController.class);

  public RegistrationController(final AccountsManager accounts,
                                final PhoneVerificationTokenManager phoneVerificationTokenManager,
                                final RegistrationLockVerificationManager registrationLockVerificationManager,
                                final RateLimiters rateLimiters,
                                final RegistrationFraudChecker registrationFraudChecker,
                                final ReceiptCredentialPresentationFactory receiptCredentialPresentationFactory,
                                final ServerZkReceiptOperations serverZkReceiptOperations,
                                final Clock clock,
                                final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {

    this.accounts = accounts;
    this.phoneVerificationTokenManager = phoneVerificationTokenManager;
    this.registrationLockVerificationManager = registrationLockVerificationManager;
    this.rateLimiters = rateLimiters;
    this.registrationFraudChecker = registrationFraudChecker;
    this.receiptCredentialPresentationFactory = receiptCredentialPresentationFactory;
    this.serverZkReceiptOperations = serverZkReceiptOperations;
    this.clock = clock;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Registers an account",
  description = """
      Registers a new account or attempts to “re-register” an existing account. It is expected that a well-behaved client
      could make up to three consecutive calls to this API:
      1. gets 423 from existing registration lock \n
      2. gets 409 from device available for transfer \n
      3. success \n
      """)
  @ApiResponse(responseCode = "200", description = "Account creation succeeded", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "Invalid session ID or receipt credential presentation")
  @ApiResponse(responseCode = "401", description = "The session identified in the request is not verified")
  @ApiResponse(responseCode = "403", description = "Verification failed for the provided Registration Recovery Password")
  @ApiResponse(responseCode = "409", description = "The caller has not explicitly elected to skip transferring data from another device, but a device transfer is technically possible")
  @ApiResponse(responseCode = "422", description = "The request did not pass validation")
  @ApiResponse(responseCode = "423", content = @Content(schema = @Schema(implementation = RegistrationLockFailure.class)))
  @ApiResponse(responseCode = "429", description = "Too many attempts", headers = @Header(
      name = "Retry-After",
      description = "If present, an positive integer indicating the number of seconds before a subsequent attempt could succeed"))
  @ApiResponse(responseCode = "499", description = "Client must support post-quantum ratchet")
  public AccountCreationResponse register(
      @Parameter(
          in = ParameterIn.HEADER,
          name = HttpHeaders.AUTHORIZATION,
          description = """
            The basic auth header. For accounts with a phone number, the username must be the e164-formatted number that is being registered.
            For accounts without a phone number, the username is ignored on a fresh registration (it must be present, however, so an arbitrary
            string is acceptable). For account recovery (re-registration) by ACI, it must be set to the ACI of the account being recovered.
            In all cases, the password is the password to be used for primary device authentication.
            """,
          required = true,
          schema = @Schema(type = "string")
      )
      @HeaderParam(HttpHeaders.AUTHORIZATION) @NotNull final BasicAuthorizationHeader authorizationHeader,
      @HeaderParam(HeaderUtils.X_SIGNAL_AGENT) final String signalAgent,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,
      @NotNull @Valid final RegistrationRequest registrationRequest,
      @Context final ContainerRequestContext requestContext)
      throws RateLimitExceededException, InterruptedException, RegistrationLockFailureException {

    final String number = authorizationHeader.getUsername();
    final String password = authorizationHeader.getPassword();

    if (!registrationRequest.isEverySignedKeyValid(userAgent)) {
      throw new WebApplicationException("Invalid signature", 422);
    }

    if (!(registrationRequest.accountAttributes().getCapabilities() != null
        ? registrationRequest.accountAttributes().getCapabilities()
        : Collections.<DeviceCapability>emptySet()).containsAll(DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES)) {

      throw new WebApplicationException("Missing required device capability", 499);
    }

    final AccountCreationResponse response;
    if (registrationRequest.pniIdentityKey() != null) {
      response = registerAccountWithNumber(number, password, registrationRequest, requestContext, userAgent, signalAgent);
    } else {
      response = registerAccountWithoutNumber(password, registrationRequest, userAgent, signalAgent);
    }

    return response;
  }

  private AccountCreationResponse registerAccountWithNumber(
      final String number,
      final String password,
      final RegistrationRequest registrationRequest,
      final ContainerRequestContext requestContext,
      final String userAgent,
      final String signalAgent)
      throws RateLimitExceededException, InterruptedException, RegistrationLockFailureException {

    if (registrationRequest.accountAttributes().getPhoneNumberIdentityRegistrationId().isEmpty()) {
      throw new WebApplicationException("PNI registration ID must be provided", 422);
    }

    rateLimiters.getRegistrationLimiter().validate(number);

    final PhoneVerificationRequest.VerificationType verificationType;
    try {
      verificationType = phoneVerificationTokenManager.verify(
          number,
          requestContext.getHeaderString(HttpHeaders.USER_AGENT),
          requestContext.getHeaderString(HttpHeaders.ACCEPT_LANGUAGE),
          (String) requestContext.getProperty(RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME),
          StringUtils.isNotBlank(registrationRequest.sessionId()) ? registrationRequest.decodeSessionId() : null,
          registrationRequest.recoveryPassword());
    } catch (final UnverifiedRegistrationSessionException e) {
      throw new NotAuthorizedException("registration session is unverified");
    } catch (final InvalidRegistrationSessionException e) {
      throw new BadRequestException(e.getMessage());
    } catch (final IOException e) {
      throw new ServiceUnavailableException(e.getMessage());
    } catch (final RecoveryPasswordVerificationFailedException e) {
      throw new ForbiddenException("recovery password could not be verified");
    }

    // There can be at most one existing account for a set of numbers in the same equivalence class, so it's sufficient
    // to find the first one.
    final Optional<Account> existingAccount = Util.getAlternateForms(number)
        .stream()
        .map(accounts::getByE164)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst();

    existingAccount.ifPresent(account -> {
      final Instant accountLastSeen = Instant.ofEpochMilli(account.getLastSeen());
      final Duration timeSinceLastSeen = Duration.between(accountLastSeen, Instant.now());
      REREGISTRATION_IDLE_DAYS_DISTRIBUTION.record(timeSinceLastSeen.toDays());
    });

    if (!registrationRequest.skipDeviceTransfer() && existingAccount.map(account -> account.hasCapability(DeviceCapability.TRANSFER)).orElse(false)) {
      // If a device transfer is possible, clients must explicitly opt out of a transfer (i.e. after prompting the user)
      // before we'll let them create a new account "from scratch"
      throw new WebApplicationException(Response.status(409, "device transfer available").build());
    }

    if (existingAccount.isPresent()) {
      assert existingAccount.get().getNumberOptional().isPresent();
      registrationLockVerificationManager.verifyRegistrationLock(existingAccount.get(),
          registrationRequest.accountAttributes().getRegistrationLock(),
          userAgent, RegistrationLockVerificationManager.Flow.REGISTRATION, verificationType);
    }

    final Account account = accounts.create(number,
        registrationRequest.accountAttributes(),
        existingAccount.map(Account::getBadges).orElseGet(ArrayList::new),
        registrationRequest.aciIdentityKey(),
        registrationRequest.pniIdentityKey(),
        new DeviceSpec(
            registrationRequest.accountAttributes().getName(),
            password,
            signalAgent,
            registrationRequest.accountAttributes().getCapabilities(),
            new DeviceIdentityInfo(
                registrationRequest.accountAttributes().getRegistrationId(),
                registrationRequest.deviceActivationRequest().aciSignedPreKey(),
                registrationRequest.deviceActivationRequest().aciPqLastResortPreKey()),
            Optional.of(new DeviceIdentityInfo(
                registrationRequest.accountAttributes().getPhoneNumberIdentityRegistrationId().get(),
                // We've already validated the presence of PNI keys by this point
                registrationRequest.deviceActivationRequest().pniSignedPreKey().get(),
                registrationRequest.deviceActivationRequest().pniPqLastResortPreKey().get())),
            registrationRequest.accountAttributes().getFetchesMessages(),
            registrationRequest.deviceActivationRequest().apnToken(),
            registrationRequest.deviceActivationRequest().gcmToken()),
        userAgent);

    if (verificationType == PhoneVerificationRequest.VerificationType.SESSION) {
      try {
        registrationFraudChecker.handleVerificationCompleted(registrationRequest.sessionId(), account);
      } catch (final Exception e) {
        logger.warn("Failed to notify fraud checker of completed verification session", e);
      }
    }

    Metrics.counter(ACCOUNT_CREATED_COUNTER_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent),
            Tag.of(COUNTRY_CODE_TAG_NAME, Util.getCountryCode(number)),
            Tag.of(REGION_CODE_TAG_NAME, Util.getRegion(number)),
            Tag.of(VERIFICATION_TYPE_TAG_NAME, verificationType.name())))
        .increment();

    final AccountIdentityResponse identityResponse = new AccountIdentityResponseBuilder(account)
        // If there was an existing account, return whether it could have had something in the storage service
        .storageCapable(existingAccount
            .map(a -> a.hasCapability(DeviceCapability.STORAGE))
            .orElse(false))
        .build();

    return new AccountCreationResponse(identityResponse, existingAccount.isPresent());
  }

  private AccountCreationResponse registerAccountWithoutNumber(
      final String password,
      final RegistrationRequest registrationRequest,
      final String userAgent,
      final String signalAgent) {

    if (!dynamicConfigurationManager.getConfiguration().getLoginPurchaseConfiguration().enabled()) {
      throw new BadRequestException("login purchases are not enabled");
    }

    if (registrationRequest.accountAttributes().recoveryPassword().isEmpty()) {
      throw new WebApplicationException("Account recovery password is required", 422);
    }

    final ReceiptCredentialPresentation receiptCredentialPresentation;
    try {
      receiptCredentialPresentation = receiptCredentialPresentationFactory
          .build(registrationRequest.receiptCredentialPresentation());
    } catch (InvalidInputException _) {
      throw new BadRequestException("Invalid receipt credential presentation");
    }
    try {
      serverZkReceiptOperations.verifyReceiptCredentialPresentation(receiptCredentialPresentation);
    } catch (VerificationFailedException _) {
      throw new BadRequestException("Receipt credential presentation verification failed");
    }

    final Instant receiptExpiration = Instant.ofEpochSecond(receiptCredentialPresentation.getReceiptExpirationTime());
    if (clock.instant().isAfter(receiptExpiration)) {
      throw new BadRequestException("Receipt is already expired");
    }

    final long receiptLevel = receiptCredentialPresentation.getReceiptLevel();
    if (receiptLevel != ReceiptLevel.LOGIN.getValue()) {
      throw new BadRequestException("Invalid receipt level");
    }

    try {
      final Account account = accounts.create(
          registrationRequest.accountAttributes(),
          Collections.emptyList(),
          registrationRequest.aciIdentityKey(),
          receiptCredentialPresentation,
          new DeviceSpec(
              registrationRequest.accountAttributes().getName(),
              password,
              signalAgent,
              registrationRequest.accountAttributes().getCapabilities(),
              new DeviceIdentityInfo(registrationRequest.accountAttributes().getRegistrationId(), registrationRequest.deviceActivationRequest()
                  .aciSignedPreKey(), registrationRequest.deviceActivationRequest().aciPqLastResortPreKey()),
              Optional.empty(),
              registrationRequest.accountAttributes().getFetchesMessages(),
              registrationRequest.deviceActivationRequest().apnToken(),
              registrationRequest.deviceActivationRequest().gcmToken()),
          userAgent);

      Metrics.counter(ACCOUNT_CREATED_COUNTER_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent),
              Tag.of(VERIFICATION_TYPE_TAG_NAME, registrationRequest.verificationType().name())))
          .increment();

      final AccountIdentityResponse accountIdentityResponse = new AccountIdentityResponseBuilder(account).build();
      return new AccountCreationResponse(accountIdentityResponse, false);
    } catch (ReceiptAlreadyRedeemedException _) {
      throw new BadRequestException("Receipt already redeemed");
    }
  }

}
