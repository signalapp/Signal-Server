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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.yammer.dropwizard.auth.Auth;
import com.yammer.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.AuthorizationHeader;
import org.whispersystems.textsecuregcm.auth.InvalidAuthorizationHeaderException;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.sms.SmsSender;
import org.whispersystems.textsecuregcm.sms.TwilioSmsSender;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.PendingAccountsManager;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

@Path("/v1/accounts")
public class AccountController {

  private final Logger logger = LoggerFactory.getLogger(AccountController.class);

  private final PendingAccountsManager pendingAccounts;
  private final AccountsManager        accounts;
  private final RateLimiters           rateLimiters;
  private final SmsSender              smsSender;

  public AccountController(PendingAccountsManager pendingAccounts,
                           AccountsManager accounts,
                           RateLimiters rateLimiters,
                           SmsSender smsSenderFactory)
  {
    this.pendingAccounts = pendingAccounts;
    this.accounts        = accounts;
    this.rateLimiters    = rateLimiters;
    this.smsSender       = smsSenderFactory;
  }

  @Timed
  @GET
  @Path("/{transport}/code/{number}")
  public Response createAccount(@PathParam("transport") String transport,
                                @PathParam("number")    String number)
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
        break;
      default:
        throw new WebApplicationException(Response.status(415).build());
    }

    VerificationCode verificationCode = generateVerificationCode();
    pendingAccounts.store(number, verificationCode.getVerificationCode());

    if (transport.equals("sms")) {
      smsSender.deliverSmsVerification(number, verificationCode.getVerificationCodeDisplay());
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
                            @Valid                          AccountAttributes accountAttributes)
      throws RateLimitExceededException
  {
    try {
      AuthorizationHeader header = AuthorizationHeader.fromFullHeader(authorizationHeader);
      String number              = header.getNumber();
      String password            = header.getPassword();

      rateLimiters.getVerifyLimiter().validate(number);

      Optional<String> storedVerificationCode = pendingAccounts.getCodeForNumber(number);

      if (!storedVerificationCode.isPresent() ||
          !verificationCode.equals(storedVerificationCode.get()))
      {
        throw new WebApplicationException(Response.status(403).build());
      }

      Device device = new Device();
      device.setNumber(number);
      device.setAuthenticationCredentials(new AuthenticationCredentials(password));
      device.setSignalingKey(accountAttributes.getSignalingKey());
      device.setSupportsSms(accountAttributes.getSupportsSms());
      device.setFetchesMessages(accountAttributes.getFetchesMessages());
      device.setDeviceId(0);

      accounts.createResetNumber(device);

      pendingAccounts.remove(number);

      logger.debug("Stored device...");
    } catch (InvalidAuthorizationHeaderException e) {
      logger.info("Bad Authorization Header", e);
      throw new WebApplicationException(Response.status(401).build());
    }
  }



  @Timed
  @PUT
  @Path("/gcm/")
  @Consumes(MediaType.APPLICATION_JSON)
  public void setGcmRegistrationId(@Auth Device device, @Valid GcmRegistrationId registrationId)  {
    device.setApnRegistrationId(null);
    device.setGcmRegistrationId(registrationId.getGcmRegistrationId());
    accounts.update(device);
  }

  @Timed
  @DELETE
  @Path("/gcm/")
  public void deleteGcmRegistrationId(@Auth Device device) {
    device.setGcmRegistrationId(null);
    accounts.update(device);
  }

  @Timed
  @PUT
  @Path("/apn/")
  @Consumes(MediaType.APPLICATION_JSON)
  public void setApnRegistrationId(@Auth Device device, @Valid ApnRegistrationId registrationId) {
    device.setApnRegistrationId(registrationId.getApnRegistrationId());
    device.setGcmRegistrationId(null);
    accounts.update(device);
  }

  @Timed
  @DELETE
  @Path("/apn/")
  public void deleteApnRegistrationId(@Auth Device device) {
    device.setApnRegistrationId(null);
    accounts.update(device);
  }

  @Timed
  @POST
  @Path("/voice/twiml/{code}")
  @Produces(MediaType.APPLICATION_XML)
  public Response getTwiml(@PathParam("code") String encodedVerificationText) {
    return Response.ok().entity(String.format(TwilioSmsSender.SAY_TWIML,
        encodedVerificationText)).build();
  }

  @VisibleForTesting protected VerificationCode generateVerificationCode() {
    try {
      SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
      int randomInt       = 100000 + random.nextInt(900000);
      return new VerificationCode(randomInt);
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }
}
