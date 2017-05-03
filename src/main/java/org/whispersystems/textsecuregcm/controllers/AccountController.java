/**
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
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.AuthorizationHeader;
import org.whispersystems.textsecuregcm.auth.AuthorizationToken;
import org.whispersystems.textsecuregcm.auth.AuthorizationTokenGenerator;
import org.whispersystems.textsecuregcm.auth.InvalidAuthorizationHeaderException;
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.providers.TimeProvider;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sms.TwilioSmsSender;
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
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.auth.Auth;

@Path("/v1/accounts")
public class AccountController {

  private final Logger         logger         = LoggerFactory.getLogger(AccountController.class);
  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          newUserMeter   = metricRegistry.meter(name(AccountController.class, "brand_new_user"));

  private final PendingAccountsManager                pendingAccounts;
  private final AccountsManager                       accounts;
  private final RateLimiters                          rateLimiters;
  private final SmsSender                             smsSender;
  private final MessagesManager                       messagesManager;
  private final TimeProvider                          timeProvider;
  private final Optional<AuthorizationTokenGenerator> tokenGenerator;
  private final TurnTokenGenerator                    turnTokenGenerator;
  private final Map<String, Integer>                  testDevices;

  public AccountController(PendingAccountsManager pendingAccounts,
                           AccountsManager accounts,
                           RateLimiters rateLimiters,
                           SmsSender smsSenderFactory,
                           MessagesManager messagesManager,
                           TimeProvider timeProvider,
                           Optional<byte[]> authorizationKey,
                           TurnTokenGenerator turnTokenGenerator,
                           Map<String, Integer> testDevices)
  {
    this.pendingAccounts    = pendingAccounts;
    this.accounts           = accounts;
    this.rateLimiters       = rateLimiters;
    this.smsSender          = smsSenderFactory;
    this.messagesManager    = messagesManager;
    this.timeProvider       = timeProvider;
    this.testDevices        = testDevices;
    this.turnTokenGenerator = turnTokenGenerator;

    if (authorizationKey.isPresent()) {
      tokenGenerator = Optional.of(new AuthorizationTokenGenerator(authorizationKey.get()));
    } else {
      tokenGenerator = Optional.absent();
    }
  }

  @Timed
  @GET
  @Path("/{transport}/code/{number}")
  public Response createAccount(@PathParam("transport") String transport,
                                @PathParam("number")    String number,
                                @QueryParam("client")   Optional<String> client)
      throws IOException, RateLimitExceededException
  {
    if (!Util.isValidNumber(number)) {
      logger.debug("Invalid number: " + number);
      throw new WebApplicationException(Response.status(400).build());
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
      smsSender.deliverVoxVerification(number, verificationCode.getVerificationCodeSpeech());
    }

    return Response.ok().build();
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
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

      if (accounts.isRelayListed(number)) {
        throw new WebApplicationException(Response.status(417).build());
      }

      createAccount(number, password, userAgent, accountAttributes);
    } catch (InvalidAuthorizationHeaderException e) {
      logger.info("Bad Authorization Header", e);
      throw new WebApplicationException(Response.status(401).build());
    }
  }

  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/token/{verification_token}")
  public void verifyToken(@PathParam("verification_token") String verificationToken,
                          @HeaderParam("Authorization")    String authorizationHeader,
                          @HeaderParam("X-Signal-Agent")   String userAgent,
                          @Valid                           AccountAttributes accountAttributes)
      throws RateLimitExceededException
  {
    try {
      AuthorizationHeader header   = AuthorizationHeader.fromFullHeader(authorizationHeader);
      String              number   = header.getNumber();
      String              password = header.getPassword();

      rateLimiters.getVerifyLimiter().validate(number);

      if (!tokenGenerator.isPresent()) {
        logger.debug("Attempt to authorize with key but not configured...");
        throw new WebApplicationException(Response.status(403).build());
      }

      if (!tokenGenerator.get().isValid(verificationToken, number, timeProvider.getCurrentTimeMillis())) {
        throw new WebApplicationException(Response.status(403).build());
      }

      createAccount(number, password, userAgent, accountAttributes);
    } catch (InvalidAuthorizationHeaderException e) {
      logger.info("Bad authorization header", e);
      throw new WebApplicationException(Response.status(401).build());
    }
  }

  @Timed
  @GET
  @Path("/token/")
  @Produces(MediaType.APPLICATION_JSON)
  public AuthorizationToken verifyToken(@Auth Account account)
      throws RateLimitExceededException
  {
    if (!tokenGenerator.isPresent()) {
      logger.debug("Attempt to authorize with key but not configured...");
      throw new WebApplicationException(Response.status(404).build());
    }

    return tokenGenerator.get().generateFor(account.getNumber());
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
    Device device = account.getAuthenticatedDevice().get();

    if (device.getGcmId() != null &&
        device.getGcmId().equals(registrationId.getGcmRegistrationId()))
    {
      return;
    }

    device.setApnId(null);
    device.setVoipApnId(null);
    device.setGcmId(registrationId.getGcmRegistrationId());

    if (registrationId.isWebSocketChannel()) device.setFetchesMessages(true);
    else                                     device.setFetchesMessages(false);

    accounts.update(account);
  }

  @Timed
  @DELETE
  @Path("/gcm/")
  public void deleteGcmRegistrationId(@Auth Account account) {
    Device device = account.getAuthenticatedDevice().get();
    device.setGcmId(null);
    device.setFetchesMessages(false);
    accounts.update(account);
  }

  @Timed
  @PUT
  @Path("/apn/")
  @Consumes(MediaType.APPLICATION_JSON)
  public void setApnRegistrationId(@Auth Account account, @Valid ApnRegistrationId registrationId) {
    Device device = account.getAuthenticatedDevice().get();
    device.setApnId(registrationId.getApnRegistrationId());
    device.setVoipApnId(registrationId.getVoipRegistrationId());
    device.setGcmId(null);
    device.setFetchesMessages(true);
    accounts.update(account);
  }

  @Timed
  @DELETE
  @Path("/apn/")
  public void deleteApnRegistrationId(@Auth Account account) {
    Device device = account.getAuthenticatedDevice().get();
    device.setApnId(null);
    device.setFetchesMessages(false);
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
    device.setVoiceSupported(attributes.getVoice());
    device.setVideoSupported(attributes.getVideo());
    device.setRegistrationId(attributes.getRegistrationId());
    device.setSignalingKey(attributes.getSignalingKey());
    device.setUserAgent(userAgent);

    accounts.update(account);
  }

  @Timed
  @POST
  @Path("/voice/twiml/{code}")
  @Produces(MediaType.APPLICATION_XML)
  public Response getTwiml(@PathParam("code") String encodedVerificationText) {
    return Response.ok().entity(String.format(TwilioSmsSender.SAY_TWIML,
        encodedVerificationText)).build();
  }

  private void createAccount(String number, String password, String userAgent, AccountAttributes accountAttributes) {
    Device device = new Device();
    device.setId(Device.MASTER_ID);
    device.setAuthenticationCredentials(new AuthenticationCredentials(password));
    device.setSignalingKey(accountAttributes.getSignalingKey());
    device.setFetchesMessages(accountAttributes.getFetchesMessages());
    device.setRegistrationId(accountAttributes.getRegistrationId());
    device.setName(accountAttributes.getName());
    device.setVoiceSupported(accountAttributes.getVoice());
    device.setVideoSupported(accountAttributes.getVideo());
    device.setCreated(System.currentTimeMillis());
    device.setLastSeen(Util.todayInMillis());
    device.setUserAgent(userAgent);

    Account account = new Account();
    account.setNumber(number);
    account.addDevice(device);

    if (accounts.create(account)) {
      newUserMeter.mark();
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
}
