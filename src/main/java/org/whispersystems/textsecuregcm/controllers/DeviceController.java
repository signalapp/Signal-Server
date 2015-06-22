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

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.auth.AuthorizationHeader;
import org.whispersystems.textsecuregcm.auth.InvalidAuthorizationHeaderException;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.DeviceInfo;
import org.whispersystems.textsecuregcm.entities.DeviceInfoList;
import org.whispersystems.textsecuregcm.entities.DeviceResponse;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.PendingDevicesManager;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.LinkedList;
import java.util.List;

import io.dropwizard.auth.Auth;

@Path("/v1/devices")
public class DeviceController {

  private final Logger logger = LoggerFactory.getLogger(DeviceController.class);

  private static final int MAX_DEVICES = 3;

  private final PendingDevicesManager pendingDevices;
  private final AccountsManager       accounts;
  private final RateLimiters          rateLimiters;

  public DeviceController(PendingDevicesManager pendingDevices,
                          AccountsManager accounts,
                          RateLimiters rateLimiters)
  {
    this.pendingDevices  = pendingDevices;
    this.accounts        = accounts;
    this.rateLimiters    = rateLimiters;
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public DeviceInfoList getDevices(@Auth Account account) {
    List<DeviceInfo> devices = new LinkedList<>();

    for (Device device : account.getDevices()) {
      devices.add(new DeviceInfo(device.getId(), device.getName(),
                                 device.getLastSeen(), device.getCreated()));
    }

    return new DeviceInfoList(devices);
  }

  @Timed
  @DELETE
  @Path("/{device_id}")
  public void removeDevice(@Auth Account account, @PathParam("device_id") long deviceId) {
    if (account.getAuthenticatedDevice().get().getId() != Device.MASTER_ID) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    account.removeDevice(deviceId);
    accounts.update(account);
  }

  @Timed
  @GET
  @Path("/provisioning/code")
  @Produces(MediaType.APPLICATION_JSON)
  public VerificationCode createDeviceToken(@Auth Account account)
      throws RateLimitExceededException, DeviceLimitExceededException
  {
    rateLimiters.getAllocateDeviceLimiter().validate(account.getNumber());

    if (account.getActiveDeviceCount() >= MAX_DEVICES) {
      throw new DeviceLimitExceededException(account.getDevices().size(), MAX_DEVICES);
    }

    if (account.getAuthenticatedDevice().get().getId() != Device.MASTER_ID) {
      throw new WebApplicationException(Response.Status.UNAUTHORIZED);
    }

    VerificationCode verificationCode = generateVerificationCode();
    pendingDevices.store(account.getNumber(), verificationCode.getVerificationCode());

    return verificationCode;
  }

  @Timed
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/{verification_code}")
  public DeviceResponse verifyDeviceToken(@PathParam("verification_code") String verificationCode,
                                          @HeaderParam("Authorization")   String authorizationHeader,
                                          @Valid                          AccountAttributes accountAttributes)
      throws RateLimitExceededException, DeviceLimitExceededException
  {
    try {
      AuthorizationHeader header = AuthorizationHeader.fromFullHeader(authorizationHeader);
      String number              = header.getNumber();
      String password            = header.getPassword();

      rateLimiters.getVerifyDeviceLimiter().validate(number);

      Optional<String> storedVerificationCode = pendingDevices.getCodeForNumber(number);

      if (!storedVerificationCode.isPresent() ||
          !MessageDigest.isEqual(verificationCode.getBytes(), storedVerificationCode.get().getBytes()))
      {
        throw new WebApplicationException(Response.status(403).build());
      }

      Optional<Account> account = accounts.get(number);

      if (!account.isPresent()) {
        throw new WebApplicationException(Response.status(403).build());
      }

      if (account.get().getActiveDeviceCount() >= MAX_DEVICES) {
        throw new DeviceLimitExceededException(account.get().getDevices().size(), MAX_DEVICES);
      }

      Device device = new Device();
      device.setName(accountAttributes.getName());
      device.setAuthenticationCredentials(new AuthenticationCredentials(password));
      device.setSignalingKey(accountAttributes.getSignalingKey());
      device.setFetchesMessages(accountAttributes.getFetchesMessages());
      device.setId(account.get().getNextDeviceId());
      device.setRegistrationId(accountAttributes.getRegistrationId());
      device.setLastSeen(Util.todayInMillis());
      device.setCreated(System.currentTimeMillis());

      account.get().addDevice(device);
      accounts.update(account.get());

      pendingDevices.remove(number);

      return new DeviceResponse(device.getId());
    } catch (InvalidAuthorizationHeaderException e) {
      logger.info("Bad Authorization Header", e);
      throw new WebApplicationException(Response.status(401).build());
    }
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
