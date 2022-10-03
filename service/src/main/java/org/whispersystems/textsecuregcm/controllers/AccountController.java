/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.abuse.FilterAbusiveMessages;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.BasicAuthorizationHeader;
import org.whispersystems.textsecuregcm.auth.ChangesDeviceEnabledState;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.StoredRegistrationLock;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicCaptchaConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.AccountIdentifierResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.ChangePhoneNumberRequest;
import org.whispersystems.textsecuregcm.entities.ConfirmUsernameRequest;
import org.whispersystems.textsecuregcm.entities.DeviceName;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.MismatchedDevices;
import org.whispersystems.textsecuregcm.entities.RegistrationLock;
import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.entities.ReserveUsernameRequest;
import org.whispersystems.textsecuregcm.entities.ReserveUsernameResponse;
import org.whispersystems.textsecuregcm.entities.StaleDevices;
import org.whispersystems.textsecuregcm.entities.UsernameRequest;
import org.whispersystems.textsecuregcm.entities.UsernameResponse;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.PushNotification;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.recaptcha.RecaptchaClient;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sms.TwilioVerifyExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRules;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.StoredVerificationCodeManager;
import org.whispersystems.textsecuregcm.storage.UsernameNotAvailableException;
import org.whispersystems.textsecuregcm.storage.UsernameReservationNotFoundException;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.ForwardedIpUtil;
import org.whispersystems.textsecuregcm.util.Hex;
import org.whispersystems.textsecuregcm.util.ImpossiblePhoneNumberException;
import org.whispersystems.textsecuregcm.util.NonNormalizedPhoneNumberException;
import org.whispersystems.textsecuregcm.util.Optionals;
import org.whispersystems.textsecuregcm.util.UsernameGenerator;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.VerificationCode;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/accounts")
public class AccountController {

  private final Logger         logger                   = LoggerFactory.getLogger(AccountController.class);
  private final MetricRegistry metricRegistry           = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          blockedHostMeter         = metricRegistry.meter(name(AccountController.class, "blocked_host"             ));
  private final Meter          countryFilterApplicable  = metricRegistry.meter(name(AccountController.class, "country_filter_applicable"));
  private final Meter          countryFilteredHostMeter = metricRegistry.meter(name(AccountController.class, "country_limited_host"     ));
  private final Meter          rateLimitedHostMeter     = metricRegistry.meter(name(AccountController.class, "rate_limited_host"        ));
  private final Meter          rateLimitedPrefixMeter   = metricRegistry.meter(name(AccountController.class, "rate_limited_prefix"      ));
  private final Meter          captchaRequiredMeter     = metricRegistry.meter(name(AccountController.class, "captcha_required"         ));

  private static final String PUSH_CHALLENGE_COUNTER_NAME = name(AccountController.class, "pushChallenge");
  private static final String ACCOUNT_CREATE_COUNTER_NAME = name(AccountController.class, "create");
  private static final String ACCOUNT_VERIFY_COUNTER_NAME = name(AccountController.class, "verify");
  private static final String CAPTCHA_ATTEMPT_COUNTER_NAME = name(AccountController.class, "captcha");
  private static final String CHALLENGE_ISSUED_COUNTER_NAME = name(AccountController.class, "challengeIssued");

  private static final String TWILIO_VERIFY_ERROR_COUNTER_NAME = name(AccountController.class, "twilioVerifyError");
  private static final String TWILIO_VERIFY_UNDELIVERED_COUNTER_NAME = name(AccountController.class, "twilioUndelivered");

  private static final String INVALID_ACCEPT_LANGUAGE_COUNTER_NAME = name(AccountController.class, "invalidAcceptLanguage");
  private static final String NONSTANDARD_USERNAME_COUNTER_NAME = name(AccountController.class, "nonStandardUsername");

  private static final String CHALLENGE_PRESENT_TAG_NAME = "present";
  private static final String CHALLENGE_MATCH_TAG_NAME = "matches";
  private static final String COUNTRY_CODE_TAG_NAME = "countryCode";

  /**
   * @deprecated "region" conflicts with cloud provider region tags; prefer "regionCode" instead
   */
  @Deprecated
  private static final String REGION_TAG_NAME = "region";
  private static final String REGION_CODE_TAG_NAME = "regionCode";
  private static final String VERIFICATION_TRANSPORT_TAG_NAME = "transport";
  private static final String SCORE_TAG_NAME = "score";

  private static final String VERIFY_EXPERIMENT_TAG_NAME = "twilioVerify";


  private final StoredVerificationCodeManager      pendingAccounts;
  private final AccountsManager                    accounts;
  private final AbusiveHostRules                   abusiveHostRules;
  private final RateLimiters                       rateLimiters;
  private final SmsSender                          smsSender;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private final TurnTokenGenerator                 turnTokenGenerator;
  private final Map<String, Integer>               testDevices;
  private final RecaptchaClient recaptchaClient;
  private final PushNotificationManager            pushNotificationManager;
  private final ExternalServiceCredentialGenerator backupServiceCredentialGenerator;

  private final TwilioVerifyExperimentEnrollmentManager verifyExperimentEnrollmentManager;
  private final ChangeNumberManager changeNumberManager;

  public AccountController(StoredVerificationCodeManager pendingAccounts,
                           AccountsManager accounts,
                           AbusiveHostRules abusiveHostRules,
                           RateLimiters rateLimiters,
                           SmsSender smsSenderFactory,
                           DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
                           TurnTokenGenerator turnTokenGenerator,
                           Map<String, Integer> testDevices,
                           RecaptchaClient recaptchaClient,
                           PushNotificationManager pushNotificationManager,
                           TwilioVerifyExperimentEnrollmentManager verifyExperimentEnrollmentManager,
                           ChangeNumberManager changeNumberManager,
                           ExternalServiceCredentialGenerator backupServiceCredentialGenerator)
  {
    this.pendingAccounts                   = pendingAccounts;
    this.accounts                          = accounts;
    this.abusiveHostRules                  = abusiveHostRules;
    this.rateLimiters                      = rateLimiters;
    this.smsSender                         = smsSenderFactory;
    this.dynamicConfigurationManager       = dynamicConfigurationManager;
    this.testDevices                       = testDevices;
    this.turnTokenGenerator                = turnTokenGenerator;
    this.recaptchaClient = recaptchaClient;
    this.pushNotificationManager           = pushNotificationManager;
    this.verifyExperimentEnrollmentManager = verifyExperimentEnrollmentManager;
    this.backupServiceCredentialGenerator = backupServiceCredentialGenerator;
    this.changeNumberManager = changeNumberManager;
  }

  @Timed
  @GET
  @Path("/{type}/preauth/{token}/{number}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPreAuth(@PathParam("type") String pushType,
                             @PathParam("token") String pushToken,
                             @PathParam("number") String number,
                             @QueryParam("voip") @DefaultValue("true") boolean useVoip)
      throws ImpossiblePhoneNumberException, NonNormalizedPhoneNumberException {

    final PushNotification.TokenType tokenType = switch(pushType) {
      case "apn" -> useVoip ? PushNotification.TokenType.APN_VOIP : PushNotification.TokenType.APN;
      case "fcm" -> PushNotification.TokenType.FCM;
      default -> throw new BadRequestException();
    };

    Util.requireNormalizedNumber(number);

    String                 pushChallenge          = generatePushChallenge();
    StoredVerificationCode storedVerificationCode = new StoredVerificationCode(null,
                                                                               System.currentTimeMillis(),
                                                                               pushChallenge,
                                                                               null);

    pendingAccounts.store(number, storedVerificationCode);
    pushNotificationManager.sendRegistrationChallengeNotification(pushToken, tokenType, storedVerificationCode.getPushCode());

    return Response.ok().build();
  }

  @Timed
  @GET
  @Path("/{transport}/code/{number}")
  @FilterAbusiveMessages
  @Produces(MediaType.APPLICATION_JSON)
  public Response createAccount(@PathParam("transport")         String transport,
                                @PathParam("number")            String number,
                                @HeaderParam("X-Forwarded-For") String forwardedFor,
                                @HeaderParam("User-Agent")      String userAgent,
                                @HeaderParam("Accept-Language") Optional<String> acceptLanguage,
                                @QueryParam("client")           Optional<String> client,
                                @QueryParam("captcha")          Optional<String> captcha,
                                @QueryParam("challenge")        Optional<String> pushChallenge)
      throws RateLimitExceededException, ImpossiblePhoneNumberException, NonNormalizedPhoneNumberException {

    Util.requireNormalizedNumber(number);

    String sourceHost = ForwardedIpUtil.getMostRecentProxy(forwardedFor).orElseThrow();

    Optional<StoredVerificationCode> storedChallenge = pendingAccounts.getCodeForNumber(number);

    final String countryCode = Util.getCountryCode(number);
    final String region = Util.getRegion(number);

    // if there's a captcha, assess it, otherwise check if we need a captcha
    final Optional<RecaptchaClient.AssessmentResult> assessmentResult = captcha
        .map(captchaToken -> recaptchaClient.verify(captchaToken, sourceHost));

    assessmentResult.ifPresent(result ->
        Metrics.counter(CAPTCHA_ATTEMPT_COUNTER_NAME, Tags.of(
                Tag.of("success", String.valueOf(result.valid())),
                UserAgentTagUtil.getPlatformTag(userAgent),
                Tag.of(COUNTRY_CODE_TAG_NAME, countryCode),
                Tag.of(REGION_TAG_NAME, region),
                Tag.of(REGION_CODE_TAG_NAME, region),
                Tag.of(SCORE_TAG_NAME, result.score())))
            .increment());

    boolean pushChallengeMatch = pushChallengeMatches(number, pushChallenge, storedChallenge);
    if (pushChallenge.isPresent() && !pushChallengeMatch) {
      throw new WebApplicationException(Response.status(403).build());
    }

    final boolean requiresCaptcha = assessmentResult
        .map(result -> !result.valid())
        .orElseGet(() -> requiresCaptcha(number, transport, forwardedFor, sourceHost, pushChallengeMatch));

    if (requiresCaptcha) {
      captchaRequiredMeter.mark();
      Metrics.counter(CHALLENGE_ISSUED_COUNTER_NAME, Tags.of(
              UserAgentTagUtil.getPlatformTag(userAgent),
              Tag.of(COUNTRY_CODE_TAG_NAME, Util.getCountryCode(number)),
              Tag.of(REGION_TAG_NAME, Util.getRegion(number)),
              Tag.of(REGION_CODE_TAG_NAME, region)))
          .increment();
      return Response.status(402).build();
    }

    switch (transport) {
      case "sms" -> rateLimiters.getSmsDestinationLimiter().validate(number);
      case "voice" -> {
        rateLimiters.getVoiceDestinationLimiter().validate(number);
        rateLimiters.getVoiceDestinationDailyLimiter().validate(number);
      }
      default -> throw new WebApplicationException(Response.status(422).build());
    }

    VerificationCode       verificationCode       = generateVerificationCode(number);
    StoredVerificationCode storedVerificationCode = new StoredVerificationCode(verificationCode.getVerificationCode(),
        System.currentTimeMillis(),
        storedChallenge.map(StoredVerificationCode::getPushCode).orElse(null),
        storedChallenge.flatMap(StoredVerificationCode::getTwilioVerificationSid).orElse(null));

    pendingAccounts.store(number, storedVerificationCode);

    List<Locale.LanguageRange> languageRanges;
    try {
      languageRanges = acceptLanguage.map(Locale.LanguageRange::parse).orElse(Collections.emptyList());
    } catch (final IllegalArgumentException e) {
      logger.debug("Could not get acceptable languages; Accept-Language: {}; User-Agent: {}",
          acceptLanguage.orElse(""),
          userAgent,
          e);

      Metrics.counter(INVALID_ACCEPT_LANGUAGE_COUNTER_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent))).increment();
      languageRanges = Collections.emptyList();
    }

    final boolean enrolledInVerifyExperiment = verifyExperimentEnrollmentManager.isEnrolled(client, number, languageRanges, transport);
    final CompletableFuture<Optional<String>> sendVerificationWithTwilioVerifyFuture;

    if (testDevices.containsKey(number)) {
      // noop
      sendVerificationWithTwilioVerifyFuture = CompletableFuture.completedFuture(Optional.empty());
    } else if (transport.equals("sms")) {

      if (enrolledInVerifyExperiment) {
        sendVerificationWithTwilioVerifyFuture = smsSender.deliverSmsVerificationWithTwilioVerify(number, client, verificationCode.getVerificationCode(), languageRanges);
      } else {
        smsSender.deliverSmsVerification(number, client, verificationCode.getVerificationCodeDisplay());
        sendVerificationWithTwilioVerifyFuture = CompletableFuture.completedFuture(Optional.empty());
      }
    } else if (transport.equals("voice")) {

      if (enrolledInVerifyExperiment) {
        sendVerificationWithTwilioVerifyFuture = smsSender.deliverVoxVerificationWithTwilioVerify(number, verificationCode.getVerificationCode(), languageRanges);
      } else {
        smsSender.deliverVoxVerification(number, verificationCode.getVerificationCode(), languageRanges);
        sendVerificationWithTwilioVerifyFuture = CompletableFuture.completedFuture(Optional.empty());
      }

    } else {
      sendVerificationWithTwilioVerifyFuture = CompletableFuture.completedFuture(Optional.empty());
    }

    sendVerificationWithTwilioVerifyFuture.whenComplete((maybeVerificationSid, throwable) -> {
      if (throwable != null) {
        Metrics.counter(TWILIO_VERIFY_ERROR_COUNTER_NAME).increment();

        logger.warn("Error with Twilio Verify", throwable);
        return;
      }
      if (enrolledInVerifyExperiment && maybeVerificationSid.isEmpty() && assessmentResult.isPresent()) {
        Metrics.counter(TWILIO_VERIFY_UNDELIVERED_COUNTER_NAME, Tags.of(
                Tag.of(COUNTRY_CODE_TAG_NAME, countryCode),
                Tag.of(REGION_TAG_NAME, region),
                UserAgentTagUtil.getPlatformTag(userAgent),
                Tag.of(SCORE_TAG_NAME, assessmentResult.get().score())))
            .increment();
      }
      maybeVerificationSid.ifPresent(twilioVerificationSid -> {
        StoredVerificationCode storedVerificationCodeWithVerificationSid = new StoredVerificationCode(
            storedVerificationCode.getCode(),
            storedVerificationCode.getTimestamp(),
            storedVerificationCode.getPushCode(),
            twilioVerificationSid);
        pendingAccounts.store(number, storedVerificationCodeWithVerificationSid);
      });
    });

    // TODO Remove this meter when external dependencies have been resolved
    metricRegistry.meter(name(AccountController.class, "create", Util.getCountryCode(number))).mark();

    Metrics.counter(ACCOUNT_CREATE_COUNTER_NAME, Tags.of(
            UserAgentTagUtil.getPlatformTag(userAgent),
            Tag.of(COUNTRY_CODE_TAG_NAME, Util.getCountryCode(number)),
            Tag.of(REGION_TAG_NAME, Util.getRegion(number)),
            Tag.of(VERIFICATION_TRANSPORT_TAG_NAME, transport),
            Tag.of(VERIFY_EXPERIMENT_TAG_NAME, String.valueOf(enrolledInVerifyExperiment))))
        .increment();

    return Response.ok().build();
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/code/{verification_code}")
  public AccountIdentityResponse verifyAccount(@PathParam("verification_code") String verificationCode,
                                             @HeaderParam("Authorization") BasicAuthorizationHeader authorizationHeader,
                                             @HeaderParam("X-Signal-Agent") String signalAgent,
                                             @HeaderParam("User-Agent") String userAgent,
                                             @QueryParam("transfer") Optional<Boolean> availableForTransfer,
                                             @NotNull @Valid AccountAttributes accountAttributes)
      throws RateLimitExceededException, InterruptedException {

    String number = authorizationHeader.getUsername();
    String password = authorizationHeader.getPassword();

    rateLimiters.getVerifyLimiter().validate(number);

    // Note that successful verification depends on being able to find a stored verification code for the given number.
    // We check that numbers are normalized before we store verification codes, and so don't need to re-assert
    // normalization here.
    Optional<StoredVerificationCode> storedVerificationCode = pendingAccounts.getCodeForNumber(number);

    if (storedVerificationCode.isEmpty() || !storedVerificationCode.get().isValid(verificationCode)) {
      throw new WebApplicationException(Response.status(403).build());
    }

    storedVerificationCode.flatMap(StoredVerificationCode::getTwilioVerificationSid)
        .ifPresent(
            verificationSid -> smsSender.reportVerificationSucceeded(verificationSid, userAgent, "registration"));

    Optional<Account> existingAccount = accounts.getByE164(number);

    if (existingAccount.isPresent()) {
      verifyRegistrationLock(existingAccount.get(), accountAttributes.getRegistrationLock());
    }

    if (availableForTransfer.orElse(false) && existingAccount.map(Account::isTransferSupported).orElse(false)) {
      throw new WebApplicationException(Response.status(409).build());
    }

    rateLimiters.getVerifyLimiter().clear(number);

    Account account = accounts.create(number, password, signalAgent, accountAttributes,
        existingAccount.map(Account::getBadges).orElseGet(ArrayList::new));

    metricRegistry.meter(name(AccountController.class, "verify", Util.getCountryCode(number))).mark();

    Metrics.counter(ACCOUNT_VERIFY_COUNTER_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent),
            Tag.of(COUNTRY_CODE_TAG_NAME, Util.getCountryCode(number)),
            Tag.of(REGION_TAG_NAME, Util.getRegion(number)),
            Tag.of(REGION_CODE_TAG_NAME, Util.getRegion(number)),
            Tag.of(VERIFY_EXPERIMENT_TAG_NAME, String.valueOf(storedVerificationCode.get().getTwilioVerificationSid().isPresent()))))
        .increment();

    return new AccountIdentityResponse(account.getUuid(),
        account.getNumber(),
        account.getPhoneNumberIdentifier(),
        account.getUsername().orElse(null),
        existingAccount.map(Account::isStorageSupported).orElse(false));
  }

  @Timed
  @PUT
  @Path("/number")
  @Produces(MediaType.APPLICATION_JSON)
  public AccountIdentityResponse changeNumber(@Auth final AuthenticatedAccount authenticatedAccount,
      @NotNull @Valid final ChangePhoneNumberRequest request,
      @HeaderParam("User-Agent") String userAgent)
      throws RateLimitExceededException, InterruptedException, ImpossiblePhoneNumberException, NonNormalizedPhoneNumberException {

    if (!authenticatedAccount.getAuthenticatedDevice().isMaster()) {
      throw new ForbiddenException();
    }

    final String number = request.number();

    // Only "bill" for rate limiting if we think there's a change to be made...
    if (!authenticatedAccount.getAccount().getNumber().equals(number)) {
      Util.requireNormalizedNumber(number);

      rateLimiters.getVerifyLimiter().validate(number);

      final Optional<StoredVerificationCode> storedVerificationCode =
          pendingAccounts.getCodeForNumber(number);

      if (storedVerificationCode.isEmpty() || !storedVerificationCode.get().isValid(request.code())) {
        throw new ForbiddenException();
      }

      storedVerificationCode.flatMap(StoredVerificationCode::getTwilioVerificationSid)
          .ifPresent(
              verificationSid -> smsSender.reportVerificationSucceeded(verificationSid, userAgent, "changeNumber"));

      final Optional<Account> existingAccount = accounts.getByE164(number);

      if (existingAccount.isPresent()) {
        verifyRegistrationLock(existingAccount.get(), request.registrationLock());
      }

      rateLimiters.getVerifyLimiter().clear(number);
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
          updatedAccount.getUsername().orElse(null),
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
  @GET
  @Path("/turn/")
  @Produces(MediaType.APPLICATION_JSON)
  public TurnToken getTurnToken(@Auth AuthenticatedAccount auth) throws RateLimitExceededException {
    rateLimiters.getTurnLimiter().validate(auth.getAccount().getUuid());
    return turnTokenGenerator.generate(auth.getAccount().getNumber());
  }

  @Timed
  @PUT
  @Path("/gcm/")
  @Consumes(MediaType.APPLICATION_JSON)
  @ChangesDeviceEnabledState
  public void setGcmRegistrationId(@Auth DisabledPermittedAuthenticatedAccount disabledPermittedAuth,
      @NotNull @Valid GcmRegistrationId registrationId) {
    Account account = disabledPermittedAuth.getAccount();
    Device device = disabledPermittedAuth.getAuthenticatedDevice();

    if (device.getGcmId() != null &&
        device.getGcmId().equals(registrationId.getGcmRegistrationId())) {
      return;
    }

    accounts.updateDevice(account, device.getId(), d -> {
      d.setApnId(null);
      d.setVoipApnId(null);
      d.setGcmId(registrationId.getGcmRegistrationId());
      d.setFetchesMessages(false);
    });
  }

  @Timed
  @DELETE
  @Path("/gcm/")
  @ChangesDeviceEnabledState
  public void deleteGcmRegistrationId(@Auth DisabledPermittedAuthenticatedAccount disabledPermittedAuth) {
    Account account = disabledPermittedAuth.getAccount();
    Device device = disabledPermittedAuth.getAuthenticatedDevice();

    accounts.updateDevice(account, device.getId(), d -> {
      d.setGcmId(null);
      d.setFetchesMessages(false);
      d.setUserAgent("OWA");
    });
  }

  @Timed
  @PUT
  @Path("/apn/")
  @Consumes(MediaType.APPLICATION_JSON)
  @ChangesDeviceEnabledState
  public void setApnRegistrationId(@Auth DisabledPermittedAuthenticatedAccount disabledPermittedAuth,
      @NotNull @Valid ApnRegistrationId registrationId) {
    Account account = disabledPermittedAuth.getAccount();
    Device device = disabledPermittedAuth.getAuthenticatedDevice();

    accounts.updateDevice(account, device.getId(), d -> {
      d.setApnId(registrationId.getApnRegistrationId());
      d.setVoipApnId(registrationId.getVoipRegistrationId());
      d.setGcmId(null);
      d.setFetchesMessages(false);
    });
  }

  @Timed
  @DELETE
  @Path("/apn/")
  @ChangesDeviceEnabledState
  public void deleteApnRegistrationId(@Auth DisabledPermittedAuthenticatedAccount disabledPermittedAuth) {
    Account account = disabledPermittedAuth.getAccount();
    Device device = disabledPermittedAuth.getAuthenticatedDevice();

    accounts.updateDevice(account, device.getId(), d -> {
      d.setApnId(null);
      d.setFetchesMessages(false);
      if (d.getId() == 1) {
        d.setUserAgent("OWI");
      } else {
        d.setUserAgent("OWP");
      }
    });
  }

  @Timed
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/registration_lock")
  public void setRegistrationLock(@Auth AuthenticatedAccount auth, @NotNull @Valid RegistrationLock accountLock) {
    AuthenticationCredentials credentials = new AuthenticationCredentials(accountLock.getRegistrationLock());

    accounts.update(auth.getAccount(),
        a -> a.setRegistrationLock(credentials.getHashedAuthenticationToken(), credentials.getSalt()));
  }

  @Timed
  @DELETE
  @Path("/registration_lock")
  public void removeRegistrationLock(@Auth AuthenticatedAccount auth) {
    accounts.update(auth.getAccount(), a -> a.setRegistrationLock(null, null));
  }

  @Timed
  @PUT
  @Path("/name/")
  public void setName(@Auth DisabledPermittedAuthenticatedAccount disabledPermittedAuth, @NotNull @Valid DeviceName deviceName) {
    Account account = disabledPermittedAuth.getAccount();
    Device device = disabledPermittedAuth.getAuthenticatedDevice();
    accounts.updateDevice(account, device.getId(), d -> d.setName(deviceName.getDeviceName()));
  }

  @Timed
  @DELETE
  @Path("/signaling_key")
  public void removeSignalingKey(@Auth DisabledPermittedAuthenticatedAccount disabledPermittedAuth) {
  }

  @Timed
  @PUT
  @Path("/attributes/")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ChangesDeviceEnabledState
  public void setAccountAttributes(@Auth DisabledPermittedAuthenticatedAccount disabledPermittedAuth,
      @HeaderParam("X-Signal-Agent") String userAgent,
      @NotNull @Valid AccountAttributes attributes) {
    Account account = disabledPermittedAuth.getAccount();
    long deviceId = disabledPermittedAuth.getAuthenticatedDevice().getId();

    accounts.update(account, a -> {
      a.getDevice(deviceId).ifPresent(d -> {
        d.setFetchesMessages(attributes.getFetchesMessages());
        d.setName(attributes.getName());
        d.setLastSeen(Util.todayInMillis());
        d.setCapabilities(attributes.getCapabilities());
        d.setRegistrationId(attributes.getRegistrationId());
        attributes.getPhoneNumberIdentityRegistrationId().ifPresent(d::setPhoneNumberIdentityRegistrationId);
        d.setUserAgent(userAgent);
      });

      a.setRegistrationLockFromAttributes(attributes);
      a.setUnidentifiedAccessKey(attributes.getUnidentifiedAccessKey());
      a.setUnrestrictedUnidentifiedAccess(attributes.isUnrestrictedUnidentifiedAccess());
      a.setDiscoverableByPhoneNumber(attributes.isDiscoverableByPhoneNumber());
    });
  }

  @GET
  @Path("/me")
  @Produces(MediaType.APPLICATION_JSON)
  public AccountIdentityResponse getMe(@Auth AuthenticatedAccount auth) {
    return whoAmI(auth);
  }

  @GET
  @Path("/whoami")
  @Produces(MediaType.APPLICATION_JSON)
  public AccountIdentityResponse whoAmI(@Auth AuthenticatedAccount auth) {
    return new AccountIdentityResponse(auth.getAccount().getUuid(),
        auth.getAccount().getNumber(),
        auth.getAccount().getPhoneNumberIdentifier(),
        auth.getAccount().getUsername().orElse(null),
        auth.getAccount().isStorageSupported());
  }

  @Timed
  @DELETE
  @Path("/username")
  @Produces(MediaType.APPLICATION_JSON)
  public void deleteUsername(@Auth AuthenticatedAccount auth) {
    accounts.clearUsername(auth.getAccount());
  }


  @Timed
  @PUT
  @Path("/username/reserved")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public ReserveUsernameResponse reserveUsername(@Auth AuthenticatedAccount auth,
      @HeaderParam("X-Signal-Agent") String userAgent,
      @NotNull @Valid ReserveUsernameRequest usernameRequest) throws RateLimitExceededException {

    rateLimiters.getUsernameReserveLimiter().validate(auth.getAccount().getUuid());

    try {
      final AccountsManager.UsernameReservation reservation = accounts.reserveUsername(
          auth.getAccount(),
          usernameRequest.nickname()
      );
      return new ReserveUsernameResponse(reservation.reservedUsername(), reservation.reservationToken());
    } catch (final UsernameNotAvailableException e) {
      throw new WebApplicationException(Status.CONFLICT);
    }
  }

  @Timed
  @PUT
  @Path("/username/confirm")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public UsernameResponse confirmUsername(@Auth AuthenticatedAccount auth,
      @HeaderParam("X-Signal-Agent") String userAgent,
      @NotNull @Valid ConfirmUsernameRequest confirmRequest) throws RateLimitExceededException {
    rateLimiters.getUsernameSetLimiter().validate(auth.getAccount().getUuid());

    try {
      final Account account = accounts.confirmReservedUsername(auth.getAccount(), confirmRequest.usernameToConfirm(), confirmRequest.reservationToken());
      return account
          .getUsername()
          .map(UsernameResponse::new)
          .orElseThrow(() -> new IllegalStateException("Could not get username after setting"));
    } catch (final UsernameReservationNotFoundException e) {
      throw new WebApplicationException(Status.CONFLICT);
    } catch (final UsernameNotAvailableException e) {
      throw new WebApplicationException(Status.GONE);
    }
  }

  @Timed
  @PUT
  @Path("/username")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public UsernameResponse setUsername(
      @Auth AuthenticatedAccount auth,
      @HeaderParam("X-Signal-Agent") String userAgent,
      @NotNull @Valid UsernameRequest usernameRequest) throws RateLimitExceededException {
    rateLimiters.getUsernameSetLimiter().validate(auth.getAccount().getUuid());
    checkUsername(usernameRequest.existingUsername(), userAgent);

    try {
      final Account account = accounts.setUsername(auth.getAccount(), usernameRequest.nickname(),
          usernameRequest.existingUsername());
      return account
          .getUsername()
          .map(UsernameResponse::new)
          .orElseThrow(() -> new IllegalStateException("Could not get username after setting"));
    } catch (final UsernameNotAvailableException e) {
      throw new WebApplicationException(Status.CONFLICT);
    }
  }

  @Timed
  @GET
  @Path("/username/{username}")
  @Produces(MediaType.APPLICATION_JSON)
  public AccountIdentifierResponse lookupUsername(
      @HeaderParam("X-Signal-Agent") final String userAgent,
      @HeaderParam("X-Forwarded-For") final String forwardedFor,
      @PathParam("username") final String username,
      @Context final HttpServletRequest request) throws RateLimitExceededException {

    // Disallow clients from making authenticated requests to this endpoint
    if (StringUtils.isNotBlank(request.getHeader("Authorization"))) {
      throw new BadRequestException();
    }

    rateLimitByClientIp(rateLimiters.getUsernameLookupLimiter(), forwardedFor);

    checkUsername(username, userAgent);

    return accounts
        .getByUsername(username)
        .map(Account::getUuid)
        .map(AccountIdentifierResponse::new)
        .orElseThrow(() -> new WebApplicationException(Status.NOT_FOUND));
  }

  @HEAD
  @Path("/account/{uuid}")
  public Response accountExists(
      @HeaderParam("X-Forwarded-For") final String forwardedFor,
      @PathParam("uuid") final UUID uuid,
      @Context HttpServletRequest request) throws RateLimitExceededException {

    // Disallow clients from making authenticated requests to this endpoint
    if (StringUtils.isNotBlank(request.getHeader("Authorization"))) {
      throw new BadRequestException();
    }
    rateLimitByClientIp(rateLimiters.getCheckAccountExistenceLimiter(), forwardedFor);

    final Status status = accounts.getByAccountIdentifier(uuid)
        .or(() -> accounts.getByPhoneNumberIdentifier(uuid))
        .isPresent() ? Status.OK : Status.NOT_FOUND;

    return Response.status(status).build();
  }

  private void rateLimitByClientIp(final RateLimiter rateLimiter, final String forwardedFor) throws RateLimitExceededException {
    final String mostRecentProxy = ForwardedIpUtil.getMostRecentProxy(forwardedFor)
        .orElseThrow(() -> {
          // Missing/malformed Forwarded-For, so we cannot check for a rate-limit.
          // This shouldn't happen, so conservatively assume we're over the rate-limit
          // and indicate that the client should retry
          logger.error("Missing/bad Forwarded-For: {}", forwardedFor);
          return new RateLimitExceededException(Duration.ofHours(1));
        });

    rateLimiter.validate(mostRecentProxy);
  }

  private void verifyRegistrationLock(final Account existingAccount, @Nullable final String clientRegistrationLock)
      throws RateLimitExceededException, WebApplicationException {

    final StoredRegistrationLock existingRegistrationLock = existingAccount.getRegistrationLock();
    final ExternalServiceCredentials existingBackupCredentials =
        backupServiceCredentialGenerator.generateFor(existingAccount.getUuid().toString());

    if (existingRegistrationLock.requiresClientRegistrationLock()) {
      if (!Util.isEmpty(clientRegistrationLock)) {
        rateLimiters.getPinLimiter().validate(existingAccount.getNumber());
      }

      if (!existingRegistrationLock.verify(clientRegistrationLock)) {
        throw new WebApplicationException(Response.status(423)
            .entity(new RegistrationLockFailure(existingRegistrationLock.getTimeRemaining(),
                existingRegistrationLock.needsFailureCredentials() ? existingBackupCredentials : null))
            .build());
      }

      rateLimiters.getPinLimiter().clear(existingAccount.getNumber());
    }
  }

  private boolean pushChallengeMatches(
      final String number,
      final Optional<String> pushChallenge,
      final Optional<StoredVerificationCode> storedVerificationCode) {
    final String countryCode = Util.getCountryCode(number);
    final String region = Util.getRegion(number);
    Optional<String> storedPushChallenge = storedVerificationCode.map(StoredVerificationCode::getPushCode);
    boolean match = Optionals.zipWith(pushChallenge, storedPushChallenge, String::equals).orElse(false);
    Metrics.counter(PUSH_CHALLENGE_COUNTER_NAME,
            COUNTRY_CODE_TAG_NAME, countryCode,
            REGION_TAG_NAME, region,
            REGION_CODE_TAG_NAME, region,
            CHALLENGE_PRESENT_TAG_NAME, Boolean.toString(pushChallenge.isPresent()),
            CHALLENGE_MATCH_TAG_NAME, Boolean.toString(match))
        .increment();
    return match;
  }

  private boolean requiresCaptcha(String number, String transport, String forwardedFor, String sourceHost, boolean pushChallengeMatch) {
    if (testDevices.containsKey(number)) {
      return false;
    }

    if (!pushChallengeMatch) {
      return true;
    }

    final String countryCode = Util.getCountryCode(number);
    final String region = Util.getRegion(number);

    DynamicCaptchaConfiguration captchaConfig = dynamicConfigurationManager.getConfiguration()
        .getCaptchaConfiguration();

    boolean countryFiltered = captchaConfig.getSignupCountryCodes().contains(countryCode) ||
        captchaConfig.getSignupRegions().contains(region);

    if (abusiveHostRules.isBlocked(sourceHost)) {
      blockedHostMeter.mark();
      logger.info("Blocked host: {}, {}, {} ({})", transport, number, sourceHost, forwardedFor);
      if (countryFiltered) {
        // this host was caught in the abusiveHostRules filter, but
        // would be caught by country filter as well
        countryFilterApplicable.mark();
      }
      return true;
    }

    try {
      rateLimiters.getSmsVoiceIpLimiter().validate(sourceHost);
    } catch (RateLimitExceededException e) {
      logger.info("Rate limit exceeded: {}, {}, {} ({})", transport, number, sourceHost, forwardedFor);
      rateLimitedHostMeter.mark();
      if (shouldAutoBlock(sourceHost)) {
        logger.info("Auto-block: {}", sourceHost);
        abusiveHostRules.setBlockedHost(sourceHost);
      }
      return true;
    }

    try {
      rateLimiters.getSmsVoicePrefixLimiter().validate(Util.getNumberPrefix(number));
    } catch (RateLimitExceededException e) {
      logger.info("Prefix rate limit exceeded: {}, {}, {} ({})", transport, number, sourceHost, forwardedFor);
      rateLimitedPrefixMeter.mark();
      if (shouldAutoBlock(sourceHost)) {
        logger.info("Auto-block: {}", sourceHost);
        abusiveHostRules.setBlockedHost(sourceHost);
      }
      return true;
    }

    if (countryFiltered) {
      countryFilteredHostMeter.mark();
      return true;
    }
    return false;
  }

  @Timed
  @DELETE
  @Path("/me")
  public void deleteAccount(@Auth AuthenticatedAccount auth) throws InterruptedException {
    accounts.delete(auth.getAccount(), AccountsManager.DeletionReason.USER_REQUEST);
  }

  private void checkUsername(final String username, final String userAgent) {
    if (StringUtils.isNotBlank(username) && !UsernameGenerator.isStandardFormat(username)) {
      // Technically, a username may not be in the nickname#discriminator format
      // if created through some out-of-band mechanism, but it is atypical.
      Metrics.counter(NONSTANDARD_USERNAME_COUNTER_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent)))
          .increment();
    }
  }

  private boolean shouldAutoBlock(String sourceHost) {
    try {
      rateLimiters.getAutoBlockLimiter().validate(sourceHost);
    } catch (RateLimitExceededException e) {
      return true;
    }

    return false;
  }

  @VisibleForTesting protected
  VerificationCode generateVerificationCode(String number) {
    if (testDevices.containsKey(number)) {
      return new VerificationCode(testDevices.get(number));
    }

    SecureRandom random = new SecureRandom();
    int randomInt       = 100000 + random.nextInt(900000);
    return new VerificationCode(randomInt);
  }

  private String generatePushChallenge() {
    SecureRandom random    = new SecureRandom();
    byte[]       challenge = new byte[16];
    random.nextBytes(challenge);

    return Hex.toStringCondensed(challenge);
  }
}
