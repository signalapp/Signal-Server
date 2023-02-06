/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
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
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.BasicAuthorizationHeader;
import org.whispersystems.textsecuregcm.auth.ChangesDeviceEnabledState;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.captcha.AssessmentResult;
import org.whispersystems.textsecuregcm.captcha.CaptchaChecker;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicCaptchaConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.AccountIdentifierResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.ChangePhoneNumberRequest;
import org.whispersystems.textsecuregcm.entities.ConfirmUsernameHashRequest;
import org.whispersystems.textsecuregcm.entities.DeviceName;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.MismatchedDevices;
import org.whispersystems.textsecuregcm.entities.RegistrationLock;
import org.whispersystems.textsecuregcm.entities.ReserveUsernameHashRequest;
import org.whispersystems.textsecuregcm.entities.ReserveUsernameHashResponse;
import org.whispersystems.textsecuregcm.entities.StaleDevices;
import org.whispersystems.textsecuregcm.entities.UsernameHashResponse;
import org.whispersystems.textsecuregcm.limits.RateLimitedByIp;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.PushNotification;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.registration.ClientType;
import org.whispersystems.textsecuregcm.registration.MessageTransport;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import org.whispersystems.textsecuregcm.spam.FilterSpam;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.StoredVerificationCodeManager;
import org.whispersystems.textsecuregcm.storage.UsernameHashNotAvailableException;
import org.whispersystems.textsecuregcm.storage.UsernameReservationNotFoundException;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.ImpossiblePhoneNumberException;
import org.whispersystems.textsecuregcm.util.NonNormalizedPhoneNumberException;
import org.whispersystems.textsecuregcm.util.Optionals;
import org.whispersystems.textsecuregcm.util.Util;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/accounts")
public class AccountController {
  public static final int MAXIMUM_USERNAME_HASHES_LIST_LENGTH = 20;
  public static final int USERNAME_HASH_LENGTH = 32;
  private final Logger         logger                   = LoggerFactory.getLogger(AccountController.class);
  private final MetricRegistry metricRegistry           = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          countryFilteredHostMeter = metricRegistry.meter(name(AccountController.class, "country_limited_host"     ));
  private final Meter          rateLimitedHostMeter     = metricRegistry.meter(name(AccountController.class, "rate_limited_host"        ));
  private final Meter          rateLimitedPrefixMeter   = metricRegistry.meter(name(AccountController.class, "rate_limited_prefix"      ));
  private final Meter          captchaRequiredMeter     = metricRegistry.meter(name(AccountController.class, "captcha_required"         ));

  private static final String PUSH_CHALLENGE_COUNTER_NAME = name(AccountController.class, "pushChallenge");
  private static final String ACCOUNT_CREATE_COUNTER_NAME = name(AccountController.class, "create");
  private static final String ACCOUNT_VERIFY_COUNTER_NAME = name(AccountController.class, "verify");
  private static final String CAPTCHA_ATTEMPT_COUNTER_NAME = name(AccountController.class, "captcha");
  private static final String CHALLENGE_ISSUED_COUNTER_NAME = name(AccountController.class, "challengeIssued");

  private static final DistributionSummary REREGISTRATION_IDLE_DAYS_DISTRIBUTION = DistributionSummary
      .builder(name(AccountController.class, "reregistrationIdleDays"))
      .publishPercentiles(0.75, 0.95, 0.99, 0.999)
      .distributionStatisticExpiry(Duration.ofHours(2))
      .register(Metrics.globalRegistry);

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


  private final StoredVerificationCodeManager pendingAccounts;
  private final AccountsManager accounts;
  private final RateLimiters rateLimiters;
  private final RegistrationServiceClient registrationServiceClient;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private final TurnTokenGenerator turnTokenGenerator;
  private final Map<String, Integer> testDevices;
  private final CaptchaChecker captchaChecker;
  private final PushNotificationManager pushNotificationManager;
  private final RegistrationLockVerificationManager registrationLockVerificationManager;
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;
  private final ChangeNumberManager changeNumberManager;
  private final Clock clock;


  @VisibleForTesting
  static final Duration REGISTRATION_RPC_TIMEOUT = Duration.ofSeconds(15);

  public AccountController(
      StoredVerificationCodeManager pendingAccounts,
      AccountsManager accounts,
      RateLimiters rateLimiters,
      RegistrationServiceClient registrationServiceClient,
      DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      TurnTokenGenerator turnTokenGenerator,
      Map<String, Integer> testDevices,
      CaptchaChecker captchaChecker,
      PushNotificationManager pushNotificationManager,
      ChangeNumberManager changeNumberManager,
      RegistrationLockVerificationManager registrationLockVerificationManager,
      RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
      Clock clock
  ) {
    this.pendingAccounts = pendingAccounts;
    this.accounts = accounts;
    this.rateLimiters = rateLimiters;
    this.registrationServiceClient = registrationServiceClient;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.testDevices = testDevices;
    this.turnTokenGenerator = turnTokenGenerator;
    this.captchaChecker = captchaChecker;
    this.pushNotificationManager = pushNotificationManager;
    this.registrationLockVerificationManager = registrationLockVerificationManager;
    this.changeNumberManager = changeNumberManager;
    this.registrationRecoveryPasswordsManager = registrationRecoveryPasswordsManager;
    this.clock = clock;
  }

  @Timed
  @GET
  @Path("/{type}/preauth/{token}/{number}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPreAuth(@PathParam("type") String pushType,
                             @PathParam("token") String pushToken,
                             @PathParam("number") String number,
                             @QueryParam("voip") @DefaultValue("true") boolean useVoip)
      throws ImpossiblePhoneNumberException, NonNormalizedPhoneNumberException, RateLimitExceededException {

    final PushNotification.TokenType tokenType = switch(pushType) {
      case "apn" -> useVoip ? PushNotification.TokenType.APN_VOIP : PushNotification.TokenType.APN;
      case "fcm" -> PushNotification.TokenType.FCM;
      default -> throw new BadRequestException();
    };

    Util.requireNormalizedNumber(number);

    final Phonenumber.PhoneNumber phoneNumber;
    try {
      phoneNumber = PhoneNumberUtil.getInstance().parse(number, null);
    } catch (final NumberParseException e) {
      // This should never happen since we just verified that the number is already normalized
      throw new BadRequestException("Bad phone number");
    }

    final String pushChallenge = generatePushChallenge();
    final byte[] sessionId = createRegistrationSession(phoneNumber);
    final StoredVerificationCode storedVerificationCode =
        new StoredVerificationCode(null, clock.millis(), pushChallenge, sessionId);

    pendingAccounts.store(number, storedVerificationCode);
    pushNotificationManager.sendRegistrationChallengeNotification(pushToken, tokenType, storedVerificationCode.pushCode());

    return Response.ok().build();
  }

  @Timed
  @GET
  @Path("/{transport}/code/{number}")
  @FilterSpam
  @Produces(MediaType.APPLICATION_JSON)
  public Response createAccount(@PathParam("transport")         String transport,
                                @PathParam("number")            String number,
                                @HeaderParam(HttpHeaders.X_FORWARDED_FOR) String forwardedFor,
                                @HeaderParam(HttpHeaders.USER_AGENT)      String userAgent,
                                @HeaderParam(HttpHeaders.ACCEPT_LANGUAGE) Optional<String> acceptLanguage,
                                @QueryParam("client")           Optional<String> client,
                                @QueryParam("captcha")          Optional<String> captcha,
                                @QueryParam("challenge")        Optional<String> pushChallenge)
      throws RateLimitExceededException, ImpossiblePhoneNumberException, NonNormalizedPhoneNumberException, IOException {

    Util.requireNormalizedNumber(number);

    final String sourceHost = HeaderUtils.getMostRecentProxy(forwardedFor).orElseThrow();
    final Optional<StoredVerificationCode> maybeStoredVerificationCode = pendingAccounts.getCodeForNumber(number);

    final String countryCode = Util.getCountryCode(number);
    final String region = Util.getRegion(number);

    // if there's a captcha, assess it, otherwise check if we need a captcha
    final Optional<AssessmentResult> assessmentResult = captcha.isPresent()
        ? Optional.of(captchaChecker.verify(captcha.get(), sourceHost))
        : Optional.empty();

    assessmentResult.ifPresent(result ->
        Metrics.counter(CAPTCHA_ATTEMPT_COUNTER_NAME, Tags.of(
                Tag.of("success", String.valueOf(result.valid())),
                UserAgentTagUtil.getPlatformTag(userAgent),
                Tag.of(COUNTRY_CODE_TAG_NAME, countryCode),
                Tag.of(REGION_TAG_NAME, region),
                Tag.of(REGION_CODE_TAG_NAME, region),
                Tag.of(SCORE_TAG_NAME, result.score())))
            .increment());

    final boolean pushChallengeMatch = pushChallengeMatches(number, pushChallenge, maybeStoredVerificationCode);

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

    final Phonenumber.PhoneNumber phoneNumber;

    try {
      phoneNumber = PhoneNumberUtil.getInstance().parse(number, null);
    } catch (final NumberParseException e) {
      throw new WebApplicationException(Response.status(422).build());
    }

    final MessageTransport messageTransport = switch (transport) {
      case "sms" -> MessageTransport.SMS;
      case "voice" -> MessageTransport.VOICE;
      default -> throw new WebApplicationException(Response.status(422).build());
    };

    final ClientType clientType = client.map(clientTypeString -> {
      if ("ios".equalsIgnoreCase(clientTypeString)) {
        return ClientType.IOS;
      } else if ("android-2021-03".equalsIgnoreCase(clientTypeString)) {
        return ClientType.ANDROID_WITH_FCM;
      } else if (StringUtils.startsWithIgnoreCase(clientTypeString, "android")) {
        return ClientType.ANDROID_WITHOUT_FCM;
      } else {
        return ClientType.UNKNOWN;
      }
    }).orElse(ClientType.UNKNOWN);

    // During the transition to explicit session creation, some previously-stored records may not have a session ID;
    // after the transition, we can assume that any existing record has an associated session ID.
    final byte[] sessionId =  maybeStoredVerificationCode.isPresent() && maybeStoredVerificationCode.get().sessionId() != null ?
        maybeStoredVerificationCode.get().sessionId() : createRegistrationSession(phoneNumber);

    sendVerificationCode(sessionId, messageTransport, clientType, acceptLanguage);

    final StoredVerificationCode storedVerificationCode = new StoredVerificationCode(null,
        clock.millis(),
        maybeStoredVerificationCode.map(StoredVerificationCode::pushCode).orElse(null),
        sessionId);

    pendingAccounts.store(number, storedVerificationCode);

    Metrics.counter(ACCOUNT_CREATE_COUNTER_NAME, Tags.of(
            UserAgentTagUtil.getPlatformTag(userAgent),
            Tag.of(COUNTRY_CODE_TAG_NAME, Util.getCountryCode(number)),
            Tag.of(REGION_TAG_NAME, Util.getRegion(number)),
            Tag.of(VERIFICATION_TRANSPORT_TAG_NAME, transport)))
        .increment();

    return Response.ok().build();
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/code/{verification_code}")
  public AccountIdentityResponse verifyAccount(@PathParam("verification_code") String verificationCode,
                                             @HeaderParam(HttpHeaders.AUTHORIZATION) BasicAuthorizationHeader authorizationHeader,
                                             @HeaderParam(HeaderUtils.X_SIGNAL_AGENT) String signalAgent,
                                             @HeaderParam(HttpHeaders.USER_AGENT) String userAgent,
                                             @QueryParam("transfer") Optional<Boolean> availableForTransfer,
                                             @NotNull @Valid AccountAttributes accountAttributes)
      throws RateLimitExceededException, InterruptedException {

    String number = authorizationHeader.getUsername();
    String password = authorizationHeader.getPassword();

    rateLimiters.getVerifyLimiter().validate(number);

    // Note that successful verification depends on being able to find a stored verification code for the given number.
    // We check that numbers are normalized before we store verification codes, and so don't need to re-assert
    // normalization here.
    final boolean codeVerified;
    final Optional<StoredVerificationCode> maybeStoredVerificationCode = pendingAccounts.getCodeForNumber(number);

    if (maybeStoredVerificationCode.isPresent()) {
      codeVerified = checkVerificationCode(maybeStoredVerificationCode.get().sessionId(), verificationCode);
    } else {
      codeVerified = false;
    }

    if (!codeVerified) {
      throw new WebApplicationException(Response.status(403).build());
    }

    Optional<Account> existingAccount = accounts.getByE164(number);

    existingAccount.ifPresent(account -> {
      Instant accountLastSeen = Instant.ofEpochMilli(account.getLastSeen());
      Duration timeSinceLastSeen = Duration.between(accountLastSeen, Instant.now());
      REREGISTRATION_IDLE_DAYS_DISTRIBUTION.record(timeSinceLastSeen.toDays());
    });

    if (existingAccount.isPresent()) {
      registrationLockVerificationManager.verifyRegistrationLock(existingAccount.get(),
          accountAttributes.getRegistrationLock());
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
            Tag.of(REGION_CODE_TAG_NAME, Util.getRegion(number))))
        .increment();

    return new AccountIdentityResponse(account.getUuid(),
        account.getNumber(),
        account.getPhoneNumberIdentifier(),
        account.getUsernameHash().orElse(null),
        existingAccount.map(Account::isStorageSupported).orElse(false));
  }

  @Timed
  @PUT
  @Path("/number")
  @Produces(MediaType.APPLICATION_JSON)
  public AccountIdentityResponse changeNumber(@Auth final AuthenticatedAccount authenticatedAccount,
      @NotNull @Valid final ChangePhoneNumberRequest request,
      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent)
      throws RateLimitExceededException, InterruptedException, ImpossiblePhoneNumberException, NonNormalizedPhoneNumberException {

    if (!authenticatedAccount.getAuthenticatedDevice().isMaster()) {
      throw new ForbiddenException();
    }

    final String number = request.number();

    // Only "bill" for rate limiting if we think there's a change to be made...
    if (!authenticatedAccount.getAccount().getNumber().equals(number)) {
      Util.requireNormalizedNumber(number);

      rateLimiters.getVerifyLimiter().validate(number);

      final boolean codeVerified;
      final Optional<StoredVerificationCode> maybeStoredVerificationCode = pendingAccounts.getCodeForNumber(number);

      if (maybeStoredVerificationCode.isPresent()) {
        codeVerified = checkVerificationCode(maybeStoredVerificationCode.get().sessionId(), request.code());
      } else {
        codeVerified = false;
      }

      if (!codeVerified) {
        throw new ForbiddenException();
      }

      final Optional<Account> existingAccount = accounts.getByE164(number);

      if (existingAccount.isPresent()) {
        registrationLockVerificationManager.verifyRegistrationLock(existingAccount.get(), request.registrationLock());
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
    SaltedTokenHash credentials = SaltedTokenHash.generateFor(accountLock.getRegistrationLock());

    accounts.update(auth.getAccount(),
        a -> a.setRegistrationLock(credentials.hash(), credentials.salt()));
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
  public void setAccountAttributes(
      @Auth DisabledPermittedAuthenticatedAccount disabledPermittedAuth,
      @HeaderParam(HeaderUtils.X_SIGNAL_AGENT) String userAgent,
      @NotNull @Valid AccountAttributes attributes) {
    final Account account = disabledPermittedAuth.getAccount();
    final long deviceId = disabledPermittedAuth.getAuthenticatedDevice().getId();

    final Account updatedAccount = accounts.update(account, a -> {
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

    // if registration recovery password was sent to us, store it (or refresh its expiration)
    attributes.recoveryPassword().ifPresent(registrationRecoveryPassword ->
        registrationRecoveryPasswordsManager.storeForCurrentNumber(updatedAccount.getNumber(), registrationRecoveryPassword));
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
        auth.getAccount().getUsernameHash().orElse(null),
        auth.getAccount().isStorageSupported());
  }

  @Timed
  @DELETE
  @Path("/username_hash")
  @Produces(MediaType.APPLICATION_JSON)
  public void deleteUsernameHash(@Auth AuthenticatedAccount auth) {
    accounts.clearUsernameHash(auth.getAccount());
  }

  @Timed
  @PUT
  @Path("/username_hash/reserve")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public ReserveUsernameHashResponse reserveUsernameHash(@Auth AuthenticatedAccount auth,
      @HeaderParam(HeaderUtils.X_SIGNAL_AGENT) String userAgent,
      @NotNull @Valid ReserveUsernameHashRequest usernameRequest) throws RateLimitExceededException {

    rateLimiters.getUsernameReserveLimiter().validate(auth.getAccount().getUuid());

    for (byte[] hash : usernameRequest.usernameHashes()) {
      if (hash.length != USERNAME_HASH_LENGTH) {
        throw new WebApplicationException(Response.status(422).build());
      }
    }

    try {
      final AccountsManager.UsernameReservation reservation = accounts.reserveUsernameHash(
          auth.getAccount(),
          usernameRequest.usernameHashes()
      );
      return new ReserveUsernameHashResponse(reservation.reservedUsernameHash());
    } catch (final UsernameHashNotAvailableException e) {
      throw new WebApplicationException(Status.CONFLICT);
    }
  }

  @Timed
  @PUT
  @Path("/username_hash/confirm")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public UsernameHashResponse confirmUsernameHash(@Auth AuthenticatedAccount auth,
      @HeaderParam(HeaderUtils.X_SIGNAL_AGENT) String userAgent,
      @NotNull @Valid ConfirmUsernameHashRequest confirmRequest) throws RateLimitExceededException {
    rateLimiters.getUsernameSetLimiter().validate(auth.getAccount().getUuid());

    try {
      final Account account = accounts.confirmReservedUsernameHash(auth.getAccount(), confirmRequest.usernameHash());
      return account
          .getUsernameHash()
          .map(UsernameHashResponse::new)
          .orElseThrow(() -> new IllegalStateException("Could not get username after setting"));
    } catch (final UsernameReservationNotFoundException e) {
      throw new WebApplicationException(Status.CONFLICT);
    } catch (final UsernameHashNotAvailableException e) {
      throw new WebApplicationException(Status.GONE);
    }
  }

  @Timed
  @GET
  @Path("/username_hash/{usernameHash}")
  @Produces(MediaType.APPLICATION_JSON)
  @RateLimitedByIp(RateLimiters.Handle.USERNAME_LOOKUP)
  public AccountIdentifierResponse lookupUsernameHash(
      @HeaderParam(HeaderUtils.X_SIGNAL_AGENT) final String userAgent,
      @HeaderParam(HttpHeaders.X_FORWARDED_FOR) final String forwardedFor,
      @PathParam("usernameHash") final String usernameHash,
      @Context final HttpServletRequest request) throws RateLimitExceededException {

    // Disallow clients from making authenticated requests to this endpoint
    if (StringUtils.isNotBlank(request.getHeader("Authorization"))) {
      throw new BadRequestException();
    }

    rateLimitByClientIp(rateLimiters.getUsernameLookupLimiter(), forwardedFor);

    final byte[] hash;
    try {
      hash = Base64.getUrlDecoder().decode(usernameHash);
    } catch (IllegalArgumentException | AssertionError e) {
      throw new WebApplicationException(Response.status(422).build());
    }

    if (hash.length != USERNAME_HASH_LENGTH) {
      throw new WebApplicationException(Response.status(422).build());
    }

    return accounts
        .getByUsernameHash(hash)
        .map(Account::getUuid)
        .map(AccountIdentifierResponse::new)
        .orElseThrow(() -> new WebApplicationException(Status.NOT_FOUND));
  }

  @HEAD
  @Path("/account/{uuid}")
  @RateLimitedByIp(RateLimiters.Handle.CHECK_ACCOUNT_EXISTENCE)
  public Response accountExists(
      @PathParam("uuid") final UUID uuid,
      @Context HttpServletRequest request) throws RateLimitExceededException {

    // Disallow clients from making authenticated requests to this endpoint
    if (StringUtils.isNotBlank(request.getHeader("Authorization"))) {
      throw new BadRequestException();
    }

    final Status status = accounts.getByAccountIdentifier(uuid)
        .or(() -> accounts.getByPhoneNumberIdentifier(uuid))
        .isPresent() ? Status.OK : Status.NOT_FOUND;

    return Response.status(status).build();
  }

  private void rateLimitByClientIp(final RateLimiter rateLimiter, final String forwardedFor) throws RateLimitExceededException {
    final String mostRecentProxy = HeaderUtils.getMostRecentProxy(forwardedFor)
        .orElseThrow(() -> {
          // Missing/malformed Forwarded-For, so we cannot check for a rate-limit.
          // This shouldn't happen, so conservatively assume we're over the rate-limit
          // and indicate that the client should retry
          logger.error("Missing/bad Forwarded-For: {}", forwardedFor);
          return new RateLimitExceededException(Duration.ofHours(1));
        });

    rateLimiter.validate(mostRecentProxy);
  }

  @VisibleForTesting
  static boolean pushChallengeMatches(
      final String number,
      final Optional<String> pushChallenge,
      final Optional<StoredVerificationCode> storedVerificationCode) {

    final String countryCode = Util.getCountryCode(number);
    final String region = Util.getRegion(number);
    final Optional<String> storedPushChallenge = storedVerificationCode.map(StoredVerificationCode::pushCode);

    final boolean match = Optionals.zipWith(pushChallenge, storedPushChallenge, String::equals).orElse(false);

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

    try {
      rateLimiters.getSmsVoiceIpLimiter().validate(sourceHost);
    } catch (RateLimitExceededException e) {
      logger.info("Rate limit exceeded: {}, {}, {} ({})", transport, number, sourceHost, forwardedFor);
      rateLimitedHostMeter.mark();

      return true;
    }

    try {
      rateLimiters.getSmsVoicePrefixLimiter().validate(Util.getNumberPrefix(number));
    } catch (RateLimitExceededException e) {
      logger.info("Prefix rate limit exceeded: {}, {}, {} ({})", transport, number, sourceHost, forwardedFor);
      rateLimitedPrefixMeter.mark();

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

  private String generatePushChallenge() {
    SecureRandom random    = new SecureRandom();
    byte[]       challenge = new byte[16];
    random.nextBytes(challenge);

    return HexFormat.of().formatHex(challenge);
  }

  private byte[] createRegistrationSession(final Phonenumber.PhoneNumber phoneNumber) throws RateLimitExceededException {

    try {
      return registrationServiceClient.createRegistrationSession(phoneNumber, REGISTRATION_RPC_TIMEOUT).join();
    } catch (final CompletionException e) {
      rethrowRateLimitException(e);

      logger.debug("Failed to create session", e);

      // Meet legacy client expectations by "swallowing" session creation exceptions and proceeding as if we had created
      // a new session. Future operations on this "session" will always fail, but that's the legacy behavior.
      return new byte[16];
    }
  }

  private void sendVerificationCode(final byte[] sessionId,
      final MessageTransport messageTransport,
      final ClientType clientType,
      final Optional<String> acceptLanguage) throws RateLimitExceededException {

    try {
      registrationServiceClient.sendRegistrationCode(sessionId,
          messageTransport,
          clientType,
          acceptLanguage.orElse(null),
          REGISTRATION_RPC_TIMEOUT).join();
    } catch (final CompletionException e) {
      // Note that, to meet legacy client expectations, we'll ONLY rethrow rate limit exceptions. All others will be
      // swallowed silently.
      rethrowRateLimitException(e);

      logger.debug("Failed to send verification code", e);
    }
  }

  private boolean checkVerificationCode(final byte[] sessionId, final String verificationCode)
      throws RateLimitExceededException {

    try {
      return registrationServiceClient.checkVerificationCode(sessionId, verificationCode, REGISTRATION_RPC_TIMEOUT).join();
    } catch (final CompletionException e) {
      rethrowRateLimitException(e);

      // For legacy API compatibility, funnel all errors into the same return value
      return false;
    }
  }

  private void rethrowRateLimitException(final CompletionException completionException)
      throws RateLimitExceededException {

    Throwable cause = completionException;

    while (cause instanceof CompletionException) {
      cause = cause.getCause();
    }

    if (cause instanceof RateLimitExceededException rateLimitExceededException) {
      throw rateLimitExceededException;
    }
  }
}
