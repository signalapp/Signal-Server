/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.security.MessageDigest;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.PhoneVerificationRequest;
import org.whispersystems.textsecuregcm.entities.RegistrationSession;
import org.whispersystems.textsecuregcm.registration.RegistrationServiceClient;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;

public class PhoneVerificationTokenManager {

  private static final Logger logger = LoggerFactory.getLogger(PhoneVerificationTokenManager.class);
  private static final Duration REGISTRATION_RPC_TIMEOUT = Duration.ofSeconds(15);
  private static final long VERIFICATION_TIMEOUT_SECONDS = REGISTRATION_RPC_TIMEOUT.plusSeconds(1).getSeconds();

  private final RegistrationServiceClient registrationServiceClient;
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;

  public PhoneVerificationTokenManager(final RegistrationServiceClient registrationServiceClient,
      final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager) {
    this.registrationServiceClient = registrationServiceClient;
    this.registrationRecoveryPasswordsManager = registrationRecoveryPasswordsManager;
  }

  /**
   * Checks if a {@link PhoneVerificationRequest} has a token that verifies the caller has confirmed access to the e164
   * number
   *
   * @param number  the e164 presented for verification
   * @param request the request with exactly one verification token (RegistrationService sessionId or registration
   *                recovery password)
   * @return if verification was successful, returns the verification type
   * @throws BadRequestException    if the number does not match the sessionId’s number
   * @throws NotAuthorizedException if the session is not verified
   * @throws ForbiddenException     if the recovery password is not valid
   * @throws InterruptedException   if verification did not complete before a timeout
   */
  public PhoneVerificationRequest.VerificationType verify(final String number, final PhoneVerificationRequest request)
      throws InterruptedException {

    final PhoneVerificationRequest.VerificationType verificationType = request.verificationType();
    switch (verificationType) {
      case SESSION -> verifyBySessionId(number, request.decodeSessionId());
      case RECOVERY_PASSWORD -> verifyByRecoveryPassword(number, request.recoveryPassword());
    }

    return verificationType;
  }

  private void verifyBySessionId(final String number, final byte[] sessionId) throws InterruptedException {
    try {
      final RegistrationSession session = registrationServiceClient
          .getSession(sessionId, REGISTRATION_RPC_TIMEOUT)
          .get(VERIFICATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .orElseThrow(() -> new NotAuthorizedException("session not verified"));

      if (!MessageDigest.isEqual(number.getBytes(), session.number().getBytes())) {
        throw new BadRequestException("number does not match session");
      }
      if (!session.verified()) {
        throw new NotAuthorizedException("session not verified");
      }
    } catch (final CancellationException | ExecutionException | TimeoutException e) {
      logger.error("Registration service failure", e);
      throw new ServerErrorException(Response.Status.SERVICE_UNAVAILABLE);
    }
  }

  private void verifyByRecoveryPassword(final String number, final byte[] recoveryPassword)
      throws InterruptedException {
    try {
      final boolean verified = registrationRecoveryPasswordsManager.verify(number, recoveryPassword)
          .get(VERIFICATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (!verified) {
        throw new ForbiddenException("recoveryPassword couldn't be verified");
      }
    } catch (final ExecutionException | TimeoutException e) {
      throw new ServerErrorException(Response.Status.SERVICE_UNAVAILABLE);
    }
  }

}
