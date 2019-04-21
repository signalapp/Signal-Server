/*
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.AuthorizationHeader;
import org.whispersystems.textsecuregcm.auth.InvalidAuthorizationHeaderException;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.DeviceName;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.RegistrationLock;
import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.recaptcha.RecaptchaClient;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sqs.DirectoryQueue;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRule;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRules;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.storage.PendingAccountsManager;
import org.whispersystems.textsecuregcm.util.Constants;
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
import java.io.IOException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.auth.Auth;

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


  private final PendingAccountsManager                pendingAccounts;
  private final AccountsManager                       accounts;
  private final AbusiveHostRules                      abusiveHostRules;
  private final RateLimiters                          rateLimiters;
  private final SmsSender                             smsSender;
  private final DirectoryQueue                        directoryQueue;
  private final MessagesManager                       messagesManager;
  private final TurnTokenGenerator                    turnTokenGenerator;
  private final Map<String, Integer>                  testDevices;
  private final RecaptchaClient                       recaptchaClient;

  public AccountController(PendingAccountsManager pendingAccounts,
                           AccountsManager accounts,
                           AbusiveHostRules abusiveHostRules,
                           RateLimiters rateLimiters,
                           SmsSender smsSenderFactory,
                           DirectoryQueue directoryQueue,
                           MessagesManager messagesManager,
                           TurnTokenGenerator turnTokenGenerator,
                           Map<String, Integer> testDevices,
                           RecaptchaClient recaptchaClient)
  {
    this.pendingAccounts    = pendingAccounts;
    this.accounts           = accounts;
    this.abusiveHostRules   = abusiveHostRules;
    this.rateLimiters       = rateLimiters;
    this.smsSender          = smsSenderFactory;
    this.directoryQueue     = directoryQueue;
    this.messagesManager    = messagesManager;
    this.testDevices        = testDevices;
    this.turnTokenGenerator = turnTokenGenerator;
    this.recaptchaClient    = recaptchaClient;
  }

  @Timed
  @GET
  @Path("/{transport}/code/{number}")
  public Response createAccount(@PathParam("transport")         String transport,
                                @PathParam("number")            String number,
                                @HeaderParam("X-Forwarded-For") String forwardedFor,
                                @HeaderParam("Accept-Language") Optional<String> locale,
                                @QueryParam("client")           Optional<String> client,
                                @QueryParam("captcha")          Optional<String> captcha)
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

    CaptchaRequirement requirement = requiresCaptcha(number, transport, forwardedFor, requester, captcha);

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
                                                                               System.currentTimeMillis());

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
  public void verifyAccount(@PathParam("verification_code") String verificationCode,
                            @HeaderParam("Authorization")   String authorizationHeader,
                            @HeaderParam("X-Signal-Agent")  String userAgent,
                            @Valid                          AccountAttributes accountAttributes)
      throws RateLimitExceededException
  {
    try {
      AuthorizationHeader header = AuthorizationHeader.fromFullHeader(authorizationHeader);
      String number              = header.getNumber();
      String password            = header.getPassword();

      rateLimiters.getVerifyLimiter().validate(number);

      Optional<StoredVerificationCode> storedVerificationCode = pendingAccounts.getCodeForNumber(number);

      if (!storedVerificationCode.isPresent() || !storedVerificationCode.get().isValid(verificationCode)) {
        throw new WebApplicationException(Response.status(403).build());
      }

      Optional<Account> existingAccount = accounts.get(number);

      if (existingAccount.isPresent()                &&
          existingAccount.get().getPin().isPresent() &&
          System.currentTimeMillis() - existingAccount.get().getLastSeen() < TimeUnit.DAYS.toMillis(7))
      {
        rateLimiters.getVerifyLimiter().clear(number);

        long timeRemaining = TimeUnit.DAYS.toMillis(7) - (System.currentTimeMillis() - existingAccount.get().getLastSeen());

        if (accountAttributes.getPin() == null) {
          throw new WebApplicationException(Response.status(423)
                                                    .entity(new RegistrationLockFailure(timeRemaining))
                                                    .build());
        }

        rateLimiters.getPinLimiter().validate(number);

        if (!MessageDigest.isEqual(existingAccount.get().getPin().get().getBytes(), accountAttributes.getPin().getBytes())) {
          throw new WebApplicationException(Response.status(423)
                                                    .entity(new RegistrationLockFailure(timeRemaining))
                                                    .build());
        }

        rateLimiters.getPinLimiter().clear(number);
      }

      createAccount(number, password, userAgent, accountAttributes);

      metricRegistry.meter(name(AccountController.class, "verify", Util.getCountryCode(number))).mark();
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
  public void setGcmRegistrationId(@Auth Account account, @Valid GcmRegistrationId registrationId) {
    Device  device           = account.getAuthenticatedDevice().get();
    boolean wasAccountActive = account.isActive();

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

    if (!wasAccountActive && account.isActive()) {
      directoryQueue.addRegisteredUser(account.getNumber());
    }
  }

  @Timed
  @DELETE
  @Path("/gcm/")
  public void deleteGcmRegistrationId(@Auth Account account) {
    Device device = account.getAuthenticatedDevice().get();
    device.setGcmId(null);
    device.setFetchesMessages(false);

    accounts.update(account);

    if (!account.isActive()) {
      directoryQueue.deleteRegisteredUser(account.getNumber());
    }
  }

  @Timed
  @PUT
  @Path("/apn/")
  @Consumes(MediaType.APPLICATION_JSON)
  public void setApnRegistrationId(@Auth Account account, @Valid ApnRegistrationId registrationId) {
    Device  device           = account.getAuthenticatedDevice().get();
    boolean wasAccountActive = account.isActive();

    device.setApnId(registrationId.getApnRegistrationId());
    device.setVoipApnId(registrationId.getVoipRegistrationId());
    device.setGcmId(null);
    device.setFetchesMessages(false);
    accounts.update(account);

    if (!wasAccountActive && account.isActive()) {
      directoryQueue.addRegisteredUser(account.getNumber());
    }
  }

  @Timed
  @DELETE
  @Path("/apn/")
  public void deleteApnRegistrationId(@Auth Account account) {
    Device device = account.getAuthenticatedDevice().get();
    device.setApnId(null);
    device.setFetchesMessages(false);

    accounts.update(account);

    if (!account.isActive()) {
      directoryQueue.deleteRegisteredUser(account.getNumber());
    }
  }

  @Timed
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/pin/")
  public void setPin(@Auth Account account, @Valid RegistrationLock accountLock) {
    account.setPin(accountLock.getPin());
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
  public void setName(@Auth Account account, @Valid DeviceName deviceName) {
    account.getAuthenticatedDevice().get().setName(deviceName.getDeviceName());
    accounts.update(account);
  }

  @Timed
  @DELETE
  @Path("/signaling_key")
  public void removeSignalingKey(@Auth Account account) {
    account.getAuthenticatedDevice().get().setSignalingKey(null);
    accounts.update(account);
  }

  @Timed
  @PUT
  @Path("/attributes/")
  @Consumes(MediaType.APPLICATION_JSON)
  public void setAccountAttributes(@Auth Account account,
                                   @HeaderParam("X-Signal-Agent") String userAgent,
                                   @Valid AccountAttributes attributes)
  {
    Device device = account.getAuthenticatedDevice().get();

    device.setFetchesMessages(attributes.getFetchesMessages());
    device.setName(attributes.getName());
    device.setLastSeen(Util.todayInMillis());
    device.setUnauthenticatedDeliverySupported(attributes.getUnidentifiedAccessKey() != null);
    device.setRegistrationId(attributes.getRegistrationId());
    device.setSignalingKey(attributes.getSignalingKey());
    device.setUserAgent(userAgent);

    account.setPin(attributes.getPin());
    account.setUnidentifiedAccessKey(attributes.getUnidentifiedAccessKey());
    account.setUnrestrictedUnidentifiedAccess(attributes.isUnrestrictedUnidentifiedAccess());

    accounts.update(account);
  }

  private CaptchaRequirement requiresCaptcha(String number, String transport, String forwardedFor,
                                             String requester, Optional<String> captchaToken)
  {

    if (captchaToken.isPresent()) {
      boolean validToken = recaptchaClient.verify(captchaToken.get());

      if (validToken) {
        captchaSuccessMeter.mark();
        return new CaptchaRequirement(false, false);
      } else {
        captchaFailureMeter.mark();
        return new CaptchaRequirement(true, false);
      }
    }

    List<AbusiveHostRule> abuseRules = abusiveHostRules.getAbusiveHostRulesFor(requester);

    for (AbusiveHostRule abuseRule : abuseRules) {
      if (abuseRule.isBlocked()) {
        logger.info("Blocked host: " + transport + ", " + number + ", " + requester + " (" + forwardedFor + ")");
        blockedHostMeter.mark();
        return new CaptchaRequirement(true, false);
      }

      if (!abuseRule.getRegions().isEmpty()) {
        if (abuseRule.getRegions().stream().noneMatch(number::startsWith)) {
          logger.info("Restricted host: " + transport + ", " + number + ", " + requester + " (" + forwardedFor + ")");
          filteredHostMeter.mark();
          return new CaptchaRequirement(true, false);
        }
      }
    }

    try {
      rateLimiters.getSmsVoiceIpLimiter().validate(requester);
    } catch (RateLimitExceededException e) {
      logger.info("Rate limited exceeded: " + transport + ", " + number + ", " + requester + " (" + forwardedFor + ")");
      rateLimitedHostMeter.mark();
      return new CaptchaRequirement(true, true);
    }

    try {
      rateLimiters.getSmsVoicePrefixLimiter().validate(Util.getNumberPrefix(number));
    } catch (RateLimitExceededException e) {
      logger.info("Prefix rate limit exceeded: " + transport + ", " + number + ", (" + forwardedFor + ")");
      rateLimitedPrefixMeter.mark();
      return new CaptchaRequirement(true, true);
    }

    return new CaptchaRequirement(false, false);
  }

  private boolean shouldAutoBlock(String requester) {
    try {
      rateLimiters.getAutoBlockLimiter().validate(requester);
    } catch (RateLimitExceededException e) {
      return true;
    }

    return false;
  }

  private void createAccount(String number, String password, String userAgent, AccountAttributes accountAttributes) {
    Device device = new Device();
    device.setId(Device.MASTER_ID);
    device.setAuthenticationCredentials(new AuthenticationCredentials(password));
    device.setSignalingKey(accountAttributes.getSignalingKey());
    device.setFetchesMessages(accountAttributes.getFetchesMessages());
    device.setRegistrationId(accountAttributes.getRegistrationId());
    device.setName(accountAttributes.getName());
    device.setUnauthenticatedDeliverySupported(accountAttributes.getUnidentifiedAccessKey() != null);
    device.setCreated(System.currentTimeMillis());
    device.setLastSeen(Util.todayInMillis());
    device.setUserAgent(userAgent);

    Account account = new Account();
    account.setNumber(number);
    account.addDevice(device);
    account.setPin(accountAttributes.getPin());
    account.setUnidentifiedAccessKey(accountAttributes.getUnidentifiedAccessKey());
    account.setUnrestrictedUnidentifiedAccess(accountAttributes.isUnrestrictedUnidentifiedAccess());

    if (accounts.create(account)) {
      newUserMeter.mark();
    }

    if (account.isActive()) {
      directoryQueue.addRegisteredUser(number);
    } else {
      directoryQueue.deleteRegisteredUser(number);
    }

    messagesManager.clear(number);
    pendingAccounts.remove(number);
  }

  @VisibleForTesting protected VerificationCode generateVerificationCode(String number) {
    if (testDevices.containsKey(number)) {
      return new VerificationCode(testDevices.get(number));
    }

    SecureRandom random = new SecureRandom();
    int randomInt       = 100000 + random.nextInt(900000);
    return new VerificationCode(randomInt);
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
