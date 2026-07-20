/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.signal.libsignal.protocol.IdentityKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.InvalidRegistrationSessionException;
import org.whispersystems.textsecuregcm.auth.PhoneVerificationTokenManager;
import org.whispersystems.textsecuregcm.auth.RecoveryPasswordVerificationFailedException;
import org.whispersystems.textsecuregcm.auth.RegistrationLockFailureException;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.auth.UnverifiedRegistrationSessionException;
import org.whispersystems.textsecuregcm.controllers.AccountControllerV2;
import org.whispersystems.textsecuregcm.controllers.MessageDeliveryNotAllowedException;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.entities.PhoneVerificationRequest;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.MessageTooLargeException;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.logging.ImpossibleEvents;

public class ChangeNumberManager {

  private final MessageSender messageSender;
  private final AccountsManager accountsManager;
  private final PhoneVerificationTokenManager phoneVerificationTokenManager;
  private final RegistrationLockVerificationManager registrationLockVerificationManager;
  private final ChangeNumberWaitingPeriodManager changeNumberWaitingPeriodManager;
  private final RateLimiters rateLimiters;
  private final Clock clock;

  private static final Logger logger = LoggerFactory.getLogger(ChangeNumberManager.class);

  // Note that this counter name references another class deliberately in the interest of metric continuity
  private static final String CHANGE_NUMBER_COUNTER_NAME = name(AccountControllerV2.class, "changeNumber");
  private static final String VERIFICATION_TYPE_TAG_NAME = "verification";
  private static final String POST_REGISTRATION_WAITING_PERIOD_NOT_MET_COUNTER_NAME = name(ChangeNumberManager.class,
      "postRegistrationWaitingPeriodNotMet");

  public ChangeNumberManager(
      final MessageSender messageSender,
      final AccountsManager accountsManager,
      final PhoneVerificationTokenManager phoneVerificationTokenManager,
      final RegistrationLockVerificationManager registrationLockVerificationManager,
      final RateLimiters rateLimiters,
      final ChangeNumberWaitingPeriodManager changeNumberWaitingPeriodManager,
      final Clock clock) {

    this.messageSender = messageSender;
    this.accountsManager = accountsManager;
    this.phoneVerificationTokenManager = phoneVerificationTokenManager;
    this.registrationLockVerificationManager = registrationLockVerificationManager;
    this.rateLimiters = rateLimiters;
    this.changeNumberWaitingPeriodManager = changeNumberWaitingPeriodManager;
    this.clock = clock;
  }

  public Account changeNumber(final UUID accountIdentifier,
      @Nullable final byte[] sessionId,
      @Nullable final byte[] recoveryPassword,
      @Nullable final String registrationLock,
      final String number,
      final IdentityKey pniIdentityKey,
      final Map<Byte, ECSignedPreKey> deviceSignedPreKeys,
      final Map<Byte, KEMSignedPreKey> devicePqLastResortPreKeys,
      final List<IncomingMessage> deviceMessages,
      final Map<Byte, Integer> pniRegistrationIds,
      @Nullable final String userAgent,
      @Nullable final String acceptLanguage,
      @Nullable final String mostRecentProxy)
      throws InterruptedException, MismatchedDevicesException, MessageTooLargeException, RateLimitExceededException, MessageDeliveryNotAllowedException, RegistrationLockFailureException, UnverifiedRegistrationSessionException, InvalidRegistrationSessionException, IOException, RecoveryPasswordVerificationFailedException {

    final Pair<Account, Optional<Account>> accountAndMaybeExistingAccount =
        accountsManager.getAccountsForChangeNumber(accountIdentifier, number);

    final Account account = accountAndMaybeExistingAccount.first();

    // Only verify and check reglock if there's a data change to be made...
    if (!account.getNumber().equals(number)) {

      final Optional<Duration> waitingPeriodRemaining = changeNumberWaitingPeriodManager.getWaitingPeriodRemaining(account.getUuid());
      if (waitingPeriodRemaining.isPresent()) {
        Metrics.counter(POST_REGISTRATION_WAITING_PERIOD_NOT_MET_COUNTER_NAME).increment();
        throw new RateLimitExceededException(waitingPeriodRemaining.get());
      }

      rateLimiters.getRegistrationLimiter().validate(number);

      final PhoneVerificationRequest.VerificationType verificationType =
          phoneVerificationTokenManager.verify(number, userAgent, acceptLanguage, mostRecentProxy, sessionId, recoveryPassword);

      final Optional<Account> existingAccount = accountAndMaybeExistingAccount.second();

      if (existingAccount.isPresent()) {
        registrationLockVerificationManager.verifyRegistrationLock(existingAccount.get(), registrationLock,
            userAgent, RegistrationLockVerificationManager.Flow.CHANGE_NUMBER, verificationType);
      }

      Metrics.counter(CHANGE_NUMBER_COUNTER_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent),
              Tag.of(VERIFICATION_TYPE_TAG_NAME, verificationType.name())))
          .increment();
    }

    // ...but always attempt to make the change in case a client retries and needs to re-send messages
    final long serverTimestamp = clock.millis();
    final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(account.getIdentifier(IdentityType.ACI));

    // Note that these for-validation envelopes do NOT have the "updated PNI" field set, and we'll need to populate that
    // after actually changing the account's number.
    final Map<Byte, Envelope> messagesByDeviceId = deviceMessages.stream()
        .collect(Collectors.toMap(IncomingMessage::destinationDeviceId, message -> message.toEnvelope(serviceIdentifier,
            serviceIdentifier,
            Device.PRIMARY_ID,
            serverTimestamp,
            false,
            false,
            true,
            null,
            clock)));

    final Map<Byte, Integer> registrationIdsByDeviceId = deviceMessages.stream()
        .collect(Collectors.toMap(IncomingMessage::destinationDeviceId, IncomingMessage::destinationRegistrationId));

    // Make sure we can plausibly deliver the messages to other devices on the account before making any changes to the
    // account itself
    if (!messagesByDeviceId.isEmpty()) {
      MessageSender.validateIndividualMessageBundle(account,
          serviceIdentifier,
          messagesByDeviceId,
          registrationIdsByDeviceId,
          Optional.of(Device.PRIMARY_ID),
          userAgent);
    }

    final Account updatedAccount = accountsManager.changeNumber(
        account.getIdentifier(IdentityType.ACI), number, pniIdentityKey, deviceSignedPreKeys, devicePqLastResortPreKeys, pniRegistrationIds);

    if (!messagesByDeviceId.isEmpty()) {
      try {
        // Now that we've actually updated the account, populate the "updated PNI" field on all envelopes
        messagesByDeviceId.replaceAll((_, envelope) ->
            envelope.toBuilder().setUpdatedPni(UUIDUtil.toByteString(updatedAccount.getIdentifier(IdentityType.PNI)))
                .build());

        messageSender.sendMessages(updatedAccount,
            serviceIdentifier,
            messagesByDeviceId,
            registrationIdsByDeviceId,
            Optional.of(Device.PRIMARY_ID),
            userAgent);
      } catch (MismatchedDevicesException | MessageTooLargeException e) {
        ImpossibleEvents.logImpossible(logger,
            "Changed number but could not send device messages after preliminary validation succeeded {}",
            account.getIdentifier(IdentityType.ACI), e);
        throw e;
      } catch (final RuntimeException e) {
        logger.warn("Changed number but could not send all device messages for {}",
            account.getIdentifier(IdentityType.ACI), e);
        throw e;
      }
    }

    return updatedAccount;
  }
}
