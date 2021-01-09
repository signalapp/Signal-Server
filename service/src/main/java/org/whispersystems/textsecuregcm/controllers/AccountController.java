/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.Auth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.AuthorizationHeader;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAccount;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.InvalidAuthorizationHeaderException;
import org.whispersystems.textsecuregcm.auth.StoredRegistrationLock;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.AccountCreationResult;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.DeprecatedPin;
import org.whispersystems.textsecuregcm.entities.DeviceName;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.RegistrationLock;
import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.APNSender;
import org.whispersystems.textsecuregcm.push.ApnMessage;
import org.whispersystems.textsecuregcm.push.GCMSender;
import org.whispersystems.textsecuregcm.push.GcmMessage;
import org.whispersystems.textsecuregcm.recaptcha.RecaptchaClient;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRule;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRules;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PaymentAddressList;
import org.whispersystems.textsecuregcm.storage.PendingAccountsManager;
import org.whispersystems.textsecuregcm.storage.UsernamesManager;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Hex;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.textsecuregcm.util.VerificationCode;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/accounts")
public class AccountController {

  private final Logger         logger                 = LoggerFactory.getLogger(AccountController.class);
  private final MetricRegistry metricRegistry         = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          newUserMeter           = metricRegistry.meter(name(AccountController.class, "brand_new_user"     ));
  private final Meter          blockedHostMeter       = metricRegistry.meter(name(AccountController.class, "blocked_host"       ));
  private final Meter          filteredHostMeter      = metricRegistry.meter(name(AccountController.class, "filtered_host"      ));
  private final Meter          rateLimitedHostMeter   = metricRegistry.meter(name(AccountController.class, "rate_limited_host"  ));
  private final Meter          rateLimitedPrefixMeter = metricRegistry.meter(name(AccountController.class, "rate_limited_prefix"));
  private final Meter          captchaSuccessMeter    = metricRegistry.meter(name(AccountController.class, "captcha_success"    ));
  private final Meter          captchaFailureMeter    = metricRegistry.meter(name(AccountController.class, "captcha_failure"    ));


  private final PendingAccountsManager             pendingAccounts;
  private final AccountsManager                    accounts;
  private final UsernamesManager                   usernames;
  private final AbusiveHostRules                   abusiveHostRules;
  private final RateLimiters                       rateLimiters;
  private final SmsSender                          smsSender;
  private final DirectoryQueue                     directoryQueue;
  private final MessagesManager                    messagesManager;
  private final TurnTokenGenerator                 turnTokenGenerator;
  private final Map<String, Integer>               testDevices;
  private final RecaptchaClient                    recaptchaClient;
  private final GCMSender                          gcmSender;
  private final APNSender                          apnSender;
  private final ExternalServiceCredentialGenerator backupServiceCredentialGenerator;

  public AccountController(PendingAccountsManager pendingAccounts,
                           AccountsManager accounts,
                           UsernamesManager usernames,
                           AbusiveHostRules abusiveHostRules,
                           RateLimiters rateLimiters,
                           SmsSender smsSenderFactory,
                           DirectoryQueue directoryQueue,
                           MessagesManager messagesManager,
                           TurnTokenGenerator turnTokenGenerator,
                           Map<String, Integer> testDevices,
                           RecaptchaClient recaptchaClient,
                           GCMSender gcmSender,
                           APNSender apnSender,
                           ExternalServiceCredentialGenerator backupServiceCredentialGenerator)
  {
    this.pendingAccounts                   = pendingAccounts;
    this.accounts                          = accounts;
    this.usernames                         = usernames;
    this.abusiveHostRules                  = abusiveHostRules;
    this.rateLimiters                      = rateLimiters;
    this.smsSender                         = smsSenderFactory;
    this.directoryQueue                    = directoryQueue;
    this.messagesManager                   = messagesManager;
    this.testDevices                       = testDevices;
    this.turnTokenGenerator                = turnTokenGenerator;
    this.recaptchaClient                   = recaptchaClient;
    this.gcmSender                         = gcmSender;
    this.apnSender                         = apnSender;
    this.backupServiceCredentialGenerator  = backupServiceCredentialGenerator;
  }

  @Timed
  @GET
  @Path("/{type}/preauth/{token}/{number}")
  public Response getPreAuth(@PathParam("type")   String pushType,
                             @PathParam("token")  String pushToken,
                             @PathParam("number") String number)
  {
    if (!"apn".equals(pushType) && !"fcm".equals(pushType)) {
      return Response.status(400).build();
    }

    if (!Util.isValidNumber(number)) {
      return Response.status(400).build();
    }

    String                 pushChallenge          = generatePushChallenge();
    StoredVerificationCode storedVerificationCode = new StoredVerificationCode(null,
                                                                               System.currentTimeMillis(),
                                                                               pushChallenge);

    pendingAccounts.store(number, storedVerificationCode);

    if ("fcm".equals(pushType)) {
      gcmSender.sendMessage(new GcmMessage(pushToken, number, 0, GcmMessage.Type.CHALLENGE, Optional.of(storedVerificationCode.getPushCode())));
    } else if ("apn".equals(pushType)) {
      apnSender.sendMessage(new ApnMessage(pushToken, number, 0, true, Optional.of(storedVerificationCode.getPushCode())));
    } else {
      throw new AssertionError();
    }

    return Response.ok().build();
  }

  @Timed
  @GET
  @Path("/{transport}/code/{number}")
  public Response createAccount(@PathParam("transport")         String transport,
                                @PathParam("number")            String number,
                                @HeaderParam("X-Forwarded-For") String forwardedFor,
                                @HeaderParam("User-Agent")      List<String> userAgent,
                                @HeaderParam("Accept-Language") Optional<String> locale,
                                @QueryParam("client")           Optional<String> client,
                                @QueryParam("captcha")          Optional<String> captcha,
                                @QueryParam("challenge")        Optional<String> pushChallenge)
      throws RateLimitExceededException
  {
    if (!Util.isValidNumber(number)) {
      logger.info("Invalid number: " + number);
      throw new WebApplicationException(Response.status(400).build());
    }

    String requester = Arrays.stream(forwardedFor.split(","))
                             .map(String::trim)
                             .reduce((a, b) -> b)
                             .orElseThrow();

    Optional<StoredVerificationCode> storedChallenge = pendingAccounts.getCodeForNumber(number);
    CaptchaRequirement               requirement     = requiresCaptcha(number, transport, forwardedFor, requester, captcha, storedChallenge, pushChallenge);

    if (requirement.isCaptchaRequired()) {
      if (requirement.isAutoBlock() && shouldAutoBlock(requester)) {
        logger.info("Auto-block: " + requester);
        abusiveHostRules.setBlockedHost(requester, "Auto-Block");
      }

      return Response.status(402).build();
    }

    switch (transport) {
      case "sms":
        rateLimiters.getSmsDestinationLimiter().validate(number);
        break;
      case "voice":
        rateLimiters.getVoiceDestinationLimiter().validate(number);
        rateLimiters.getVoiceDestinationDailyLimiter().validate(number);
        break;
      default:
        throw new WebApplicationException(Response.status(422).build());
    }

    VerificationCode       verificationCode       = generateVerificationCode(number);
    StoredVerificationCode storedVerificationCode = new StoredVerificationCode(verificationCode.getVerificationCode(),
                                                                               System.currentTimeMillis(),
                                                                               storedChallenge.map(StoredVerificationCode::getPushCode).orElse(null));

    pendingAccounts.store(number, storedVerificationCode);

    if (testDevices.containsKey(number)) {
      // noop
    } else if (transport.equals("sms")) {
      smsSender.deliverSmsVerification(number, client, verificationCode.getVerificationCodeDisplay());
    } else if (transport.equals("voice")) {
      smsSender.deliverVoxVerification(number, verificationCode.getVerificationCode(), locale);
    }

    metricRegistry.meter(name(AccountController.class, "create", Util.getCountryCode(number))).mark();

    return Response.ok().build();
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/code/{verification_code}")
  public AccountCreationResult verifyAccount(@PathParam("verification_code") String verificationCode,
                                             @HeaderParam("Authorization")   String authorizationHeader,
                                             @HeaderParam("X-Signal-Agent")  String userAgent,
                                             @QueryParam("transfer")         Optional<Boolean> availableForTransfer,
                                             @Valid                          AccountAttributes accountAttributes)
      throws RateLimitExceededException
  {
    try {
      AuthorizationHeader header = AuthorizationHeader.fromFullHeader(authorizationHeader);
      String number              = header.getIdentifier().getNumber();
      String password            = header.getPassword();

      if (number == null) {
        throw new WebApplicationException(400);
      }

      rateLimiters.getVerifyLimiter().validate(number);

      Optional<StoredVerificationCode> storedVerificationCode = pendingAccounts.getCodeForNumber(number);

      if (storedVerificationCode.isEmpty() || !storedVerificationCode.get().isValid(verificationCode)) {
        throw new WebApplicationException(Response.status(403).build());
      }

      Optional<Account>                    existingAccount           = accounts.get(number);
      Optional<StoredRegistrationLock>     existingRegistrationLock  = existingAccount.map(Account::getRegistrationLock);
      Optional<ExternalServiceCredentials> existingBackupCredentials = existingAccount.map(Account::getUuid)
                                                                                      .map(uuid -> backupServiceCredentialGenerator.generateFor(uuid.toString()));

      if (existingRegistrationLock.isPresent() && existingRegistrationLock.get().requiresClientRegistrationLock()) {
        rateLimiters.getVerifyLimiter().clear(number);

        if (!Util.isEmpty(accountAttributes.getRegistrationLock()) || !Util.isEmpty(accountAttributes.getPin())) {
          rateLimiters.getPinLimiter().validate(number);
        }

        if (!existingRegistrationLock.get().verify(accountAttributes.getRegistrationLock(), accountAttributes.getPin())) {
          throw new WebApplicationException(Response.status(423)
                                                    .entity(new RegistrationLockFailure(existingRegistrationLock.get().getTimeRemaining(),
                                                                                        existingRegistrationLock.get().needsFailureCredentials() ? existingBackupCredentials.orElseThrow() : null))
                                                    .build());
        }

        rateLimiters.getPinLimiter().clear(number);
      }

      if (availableForTransfer.orElse(false) && existingAccount.map(Account::isTransferSupported).orElse(false)) {
        throw new WebApplicationException(Response.status(409).build());
      }

      Account account = createAccount(number, password, userAgent, accountAttributes);

      metricRegistry.meter(name(AccountController.class, "verify", Util.getCountryCode(number))).mark();

      return new AccountCreationResult(account.getUuid(), existingAccount.map(Account::isStorageSupported).orElse(false));
    } catch (InvalidAuthorizationHeaderException e) {
      logger.info("Bad Authorization Header", e);
      throw new WebApplicationException(Response.status(401).build());
    }
  }

  @Timed
  @GET
  @Path("/turn/")
  @Produces(MediaType.APPLICATION_JSON)
  public TurnToken getTurnToken(@Auth Account account) throws RateLimitExceededException {
    rateLimiters.getTurnLimiter().validate(account.getNumber());
    return turnTokenGenerator.generate();
  }

  @Timed
  @PUT
  @Path("/gcm/")
  @Consumes(MediaType.APPLICATION_JSON)
  public void setGcmRegistrationId(@Auth DisabledPermittedAccount disabledPermittedAccount, @Valid GcmRegistrationId registrationId) {
    Account account           = disabledPermittedAccount.getAccount();
    Device  device            = account.getAuthenticatedDevice().get();
    boolean wasAccountEnabled = account.isEnabled();

    if (device.getGcmId() != null &&
        device.getGcmId().equals(registrationId.getGcmRegistrationId()))
    {
      return;
    }

    device.setApnId(null);
    device.setVoipApnId(null);
    device.setGcmId(registrationId.getGcmRegistrationId());
    device.setFetchesMessages(false);

    accounts.update(account);

    if (!wasAccountEnabled && account.isEnabled()) {
      directoryQueue.refreshRegisteredUser(account);
    }
  }

  @Timed
  @DELETE
  @Path("/gcm/")
  public void deleteGcmRegistrationId(@Auth DisabledPermittedAccount disabledPermittedAccount) {
    Account account = disabledPermittedAccount.getAccount();
    Device  device  = account.getAuthenticatedDevice().get();
    device.setGcmId(null);
    device.setFetchesMessages(false);

    accounts.update(account);
    directoryQueue.refreshRegisteredUser(account);
  }

  @Timed
  @PUT
  @Path("/apn/")
  @Consumes(MediaType.APPLICATION_JSON)
  public void setApnRegistrationId(@Auth DisabledPermittedAccount disabledPermittedAccount, @Valid ApnRegistrationId registrationId) {
    Account account           = disabledPermittedAccount.getAccount();
    Device  device            = account.getAuthenticatedDevice().get();
    boolean wasAccountEnabled = account.isEnabled();

    device.setApnId(registrationId.getApnRegistrationId());
    device.setVoipApnId(registrationId.getVoipRegistrationId());
    device.setGcmId(null);
    device.setFetchesMessages(false);
    accounts.update(account);

    if (!wasAccountEnabled && account.isEnabled()) {
      directoryQueue.refreshRegisteredUser(account);
    }
  }

  @Timed
  @DELETE
  @Path("/apn/")
  public void deleteApnRegistrationId(@Auth DisabledPermittedAccount disabledPermittedAccount) {
    Account account = disabledPermittedAccount.getAccount();
    Device  device  = account.getAuthenticatedDevice().get();
    device.setApnId(null);
    device.setFetchesMessages(false);

    accounts.update(account);
    directoryQueue.refreshRegisteredUser(account);
  }

  @Timed
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/registration_lock")
  public void setRegistrationLock(@Auth Account account, @Valid RegistrationLock accountLock) {
    AuthenticationCredentials credentials = new AuthenticationCredentials(accountLock.getRegistrationLock());
    account.setRegistrationLock(credentials.getHashedAuthenticationToken(), credentials.getSalt());
    account.setPin(null);

    accounts.update(account);
  }

  @Timed
  @DELETE
  @Path("/registration_lock")
  public void removeRegistrationLock(@Auth Account account) {
    account.setRegistrationLock(null, null);
    accounts.update(account);
  }

  @Timed
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/pin/")
  public void setPin(@Auth Account account, @Valid DeprecatedPin accountLock) {
    account.setPin(accountLock.getPin());
    account.setRegistrationLock(null, null);

    accounts.update(account);
  }

  @Timed
  @DELETE
  @Path("/pin/")
  public void removePin(@Auth Account account) {
    account.setPin(null);
    accounts.update(account);
  }

  @Timed
  @PUT
  @Path("/name/")
  public void setName(@Auth DisabledPermittedAccount disabledPermittedAccount, @Valid DeviceName deviceName) {
    Account account = disabledPermittedAccount.getAccount();
    account.getAuthenticatedDevice().get().setName(deviceName.getDeviceName());
    accounts.update(account);
  }

  @Timed
  @DELETE
  @Path("/signaling_key")
  public void removeSignalingKey(@Auth DisabledPermittedAccount disabledPermittedAccount) {
    Account account = disabledPermittedAccount.getAccount();
    account.getAuthenticatedDevice().get().setSignalingKey(null);
    accounts.update(account);
  }

  @Timed
  @PUT
  @Path("/attributes/")
  @Consumes(MediaType.APPLICATION_JSON)
  public void setAccountAttributes(@Auth DisabledPermittedAccount disabledPermittedAccount,
                                   @HeaderParam("X-Signal-Agent") String userAgent,
                                   @Valid AccountAttributes attributes)
  {
    Account account = disabledPermittedAccount.getAccount();
    Device  device  = account.getAuthenticatedDevice().get();

    device.setFetchesMessages(attributes.getFetchesMessages());
    device.setName(attributes.getName());
    device.setLastSeen(Util.todayInMillis());
    device.setCapabilities(attributes.getCapabilities());
    device.setRegistrationId(attributes.getRegistrationId());
    device.setSignalingKey(attributes.getSignalingKey());
    device.setUserAgent(userAgent);

    setAccountRegistrationLockFromAttributes(account, attributes);

    final boolean hasDiscoverabilityChange = (account.isDiscoverableByPhoneNumber() != attributes.isDiscoverableByPhoneNumber());

    account.setUnidentifiedAccessKey(attributes.getUnidentifiedAccessKey());
    account.setUnrestrictedUnidentifiedAccess(attributes.isUnrestrictedUnidentifiedAccess());
    account.setPayments(attributes.getPayments());
    account.setDiscoverableByPhoneNumber(attributes.isDiscoverableByPhoneNumber());

    accounts.update(account);

    if (hasDiscoverabilityChange) {
      directoryQueue.refreshRegisteredUser(account);
    }
  }

  @GET
  @Path("/me")
  @Produces(MediaType.APPLICATION_JSON)
  public AccountCreationResult getMe(@Auth Account account) {
    return whoAmI(account);
  }

  @GET
  @Path("/whoami")
  @Produces(MediaType.APPLICATION_JSON)
  public AccountCreationResult whoAmI(@Auth Account account) {
    return new AccountCreationResult(account.getUuid(), account.isStorageSupported());
  }

  @DELETE
  @Path("/username")
  @Produces(MediaType.APPLICATION_JSON)
  public void deleteUsername(@Auth Account account) {
    usernames.delete(account.getUuid());
  }

  @PUT
  @Path("/username/{username}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response setUsername(@Auth Account account, @PathParam("username") String username) throws RateLimitExceededException {
    rateLimiters.getUsernameSetLimiter().validate(account.getUuid().toString());

    if (username == null || username.isEmpty()) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    username = username.toLowerCase();

    if (!username.matches("^[a-z_][a-z0-9_]+$")) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    if (!usernames.put(account.getUuid(), username)) {
      return Response.status(Response.Status.CONFLICT).build();
    }

    return Response.ok().build();
  }

  @Timed
  @PUT
  @Path("/payments")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public void setPayments(@Auth Account account, @Valid PaymentAddressList payments) {
    account.setPayments(payments.getPayments());
    accounts.update(account);
  }

  private CaptchaRequirement requiresCaptcha(String number, String transport, String forwardedFor,
                                             String requester,
                                             Optional<String>                 captchaToken,
                                             Optional<StoredVerificationCode> storedVerificationCode,
                                             Optional<String>                 pushChallenge)
  {

    return new CaptchaRequirement(false, false);
//    if (captchaToken.isPresent()) {
//      boolean validToken = recaptchaClient.verify(captchaToken.get(), requester);
//
//      if (validToken) {
//        captchaSuccessMeter.mark();
//        return new CaptchaRequirement(false, false);
//      } else {
//        captchaFailureMeter.mark();
//        return new CaptchaRequirement(true, false);
//      }
//    }
//
//    if (pushChallenge.isPresent()) {
//      Optional<String> storedPushChallenge = storedVerificationCode.map(StoredVerificationCode::getPushCode);
//
//      if (!pushChallenge.get().equals(storedPushChallenge.orElse(null))) {
//        return new CaptchaRequirement(true, false);
//      }
//    }
//
//    List<AbusiveHostRule> abuseRules = abusiveHostRules.getAbusiveHostRulesFor(requester);
//
//    for (AbusiveHostRule abuseRule : abuseRules) {
//      if (abuseRule.isBlocked()) {
//        logger.info("Blocked host: " + transport + ", " + number + ", " + requester + " (" + forwardedFor + ")");
//        blockedHostMeter.mark();
//        return new CaptchaRequirement(true, false);
//      }
//
//      if (!abuseRule.getRegions().isEmpty()) {
//        if (abuseRule.getRegions().stream().noneMatch(number::startsWith)) {
//          logger.info("Restricted host: " + transport + ", " + number + ", " + requester + " (" + forwardedFor + ")");
//          filteredHostMeter.mark();
//          return new CaptchaRequirement(true, false);
//        }
//      }
//    }
//
//    try {
//      rateLimiters.getSmsVoiceIpLimiter().validate(requester);
//    } catch (RateLimitExceededException e) {
//      logger.info("Rate limited exceeded: " + transport + ", " + number + ", " + requester + " (" + forwardedFor + ")");
//      rateLimitedHostMeter.mark();
//      return new CaptchaRequirement(true, true);
//    }
//
//    try {
//      rateLimiters.getSmsVoicePrefixLimiter().validate(Util.getNumberPrefix(number));
//    } catch (RateLimitExceededException e) {
//      logger.info("Prefix rate limit exceeded: " + transport + ", " + number + ", (" + forwardedFor + ")");
//      rateLimitedPrefixMeter.mark();
//      return new CaptchaRequirement(true, true);
//    }
//
//    return new CaptchaRequirement(false, false);
  }

  @Timed
  @DELETE
  @Path("/me")
  public void deleteAccount(@Auth Account account) {
    accounts.delete(account, AccountsManager.DeletionReason.USER_REQUEST);
  }

  private boolean shouldAutoBlock(String requester) {
    try {
      rateLimiters.getAutoBlockLimiter().validate(requester);
    } catch (RateLimitExceededException e) {
      return true;
    }

    return false;
  }

  private Account createAccount(String number, String password, String userAgent, AccountAttributes accountAttributes) {
    Optional<Account> maybeExistingAccount = accounts.get(number);

    Device device = new Device();
    device.setId(Device.MASTER_ID);
    device.setAuthenticationCredentials(new AuthenticationCredentials(password));
    device.setSignalingKey(accountAttributes.getSignalingKey());
    device.setFetchesMessages(accountAttributes.getFetchesMessages());
    device.setRegistrationId(accountAttributes.getRegistrationId());
    device.setName(accountAttributes.getName());
    device.setCapabilities(accountAttributes.getCapabilities());
    device.setCreated(System.currentTimeMillis());
    device.setLastSeen(Util.todayInMillis());
    device.setUserAgent(userAgent);

    Account account = new Account();
    account.setNumber(number);
    account.setUuid(UUID.randomUUID());
    account.addDevice(device);
    setAccountRegistrationLockFromAttributes(account, accountAttributes);
    account.setUnidentifiedAccessKey(accountAttributes.getUnidentifiedAccessKey());
    account.setUnrestrictedUnidentifiedAccess(accountAttributes.isUnrestrictedUnidentifiedAccess());
    account.setPayments(accountAttributes.getPayments());
    account.setDiscoverableByPhoneNumber(accountAttributes.isDiscoverableByPhoneNumber());

    if (accounts.create(account)) {
      newUserMeter.mark();
    }

    directoryQueue.refreshRegisteredUser(account);
    messagesManager.clear(number, maybeExistingAccount.map(Account::getUuid).orElse(null));
    pendingAccounts.remove(number);

    return account;
  }

  private void setAccountRegistrationLockFromAttributes(Account account, @Valid AccountAttributes attributes) {
    if (!Util.isEmpty(attributes.getPin())) {
      account.setPin(attributes.getPin());
    } else if (!Util.isEmpty(attributes.getRegistrationLock())) {
      AuthenticationCredentials credentials = new AuthenticationCredentials(attributes.getRegistrationLock());
      account.setRegistrationLock(credentials.getHashedAuthenticationToken(), credentials.getSalt());
    } else {
      account.setPin(null);
      account.setRegistrationLock(null, null);
    }
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

  private static class CaptchaRequirement {
    private final boolean captchaRequired;
    private final boolean autoBlock;

    private CaptchaRequirement(boolean captchaRequired, boolean autoBlock) {
      this.captchaRequired = captchaRequired;
      this.autoBlock       = autoBlock;
    }

    boolean isCaptchaRequired() {
      return captchaRequired;
    }

    boolean isAutoBlock() {
      return autoBlock;
    }
  }
}
