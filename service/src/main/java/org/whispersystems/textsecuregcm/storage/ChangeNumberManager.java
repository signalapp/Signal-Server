/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import com.google.common.net.HttpHeaders;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import jakarta.ws.rs.container.ContainerRequestContext;
import org.signal.libsignal.protocol.IdentityKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.PhoneVerificationTokenManager;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.controllers.AccountControllerV2;
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
import javax.annotation.Nullable;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

public class ChangeNumberManager {

  private final MessageSender messageSender;
  private final AccountsManager accountsManager;
  private final PhoneVerificationTokenManager phoneVerificationTokenManager;
  private final RegistrationLockVerificationManager registrationLockVerificationManager;
  private final RateLimiters rateLimiters;
  private final Clock clock;

  private static final Logger logger = LoggerFactory.getLogger(ChangeNumberManager.class);

  // Note that this counter name references another class deliberately in the interest of metric continuity
  private static final String CHANGE_NUMBER_COUNTER_NAME = name(AccountControllerV2.class, "changeNumber");
  private static final String VERIFICATION_TYPE_TAG_NAME = "verification";

  public ChangeNumberManager(
      final MessageSender messageSender,
      final AccountsManager accountsManager,
      final PhoneVerificationTokenManager phoneVerificationTokenManager,
      final RegistrationLockVerificationManager registrationLockVerificationManager,
      final RateLimiters rateLimiters,
      final Clock clock) {

    this.messageSender = messageSender;
    this.accountsManager = accountsManager;
    this.phoneVerificationTokenManager = phoneVerificationTokenManager;
    this.registrationLockVerificationManager = registrationLockVerificationManager;
    this.rateLimiters = rateLimiters;
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
      final ContainerRequestContext containerRequestContext)
      throws InterruptedException, MismatchedDevicesException, MessageTooLargeException, RateLimitExceededException {

    final String senderUserAgent = containerRequestContext.getHeaderString(HttpHeaders.USER_AGENT);

    final Pair<Account, Optional<Account>> accountAndMaybeExistingAccount =
        accountsManager.getAccountsForChangeNumber(accountIdentifier, number);

    final Account account = accountAndMaybeExistingAccount.first();

    // Only verify and check reglock if there's a data change to be made...
    if (!account.getNumber().equals(number)) {
      rateLimiters.getRegistrationLimiter().validate(number);

      final PhoneVerificationRequest.VerificationType verificationType =
          phoneVerificationTokenManager.verify(containerRequestContext, number, sessionId, recoveryPassword);

      final Optional<Account> existingAccount = accountAndMaybeExistingAccount.second();

      if (existingAccount.isPresent()) {
        registrationLockVerificationManager.verifyRegistrationLock(existingAccount.get(), registrationLock,
            senderUserAgent, RegistrationLockVerificationManager.Flow.CHANGE_NUMBER, verificationType);
      }

      Metrics.counter(CHANGE_NUMBER_COUNTER_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(senderUserAgent),
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
          senderUserAgent);
    }

    final Account updatedAccount = accountsManager.changeNumber(
        account.getIdentifier(IdentityType.ACI), number, pniIdentityKey, deviceSignedPreKeys, devicePqLastResortPreKeys, pniRegistrationIds);

    try {
      // Now that we've actually updated the account, populate the "updated PNI" field on all envelopes
      final String updatedPniString = updatedAccount.getIdentifier(IdentityType.PNI).toString();

      messagesByDeviceId.replaceAll((_, envelope) ->
          envelope.toBuilder().setUpdatedPni(updatedPniString).build());

      messageSender.sendMessages(updatedAccount,
          serviceIdentifier,
          messagesByDeviceId,
          registrationIdsByDeviceId,
          Optional.of(Device.PRIMARY_ID),
          senderUserAgent);
    } catch (final RuntimeException e) {
      logger.warn("Changed number but could not send all device messages for {}", account.getIdentifier(IdentityType.ACI), e);
      throw e;
    }

    return updatedAccount;
  }
}
