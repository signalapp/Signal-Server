/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.messaging.AndroidConfig;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.FirebaseMessagingException;
import com.google.firebase.messaging.Message;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Util;

public class FcmSender {

  private final Logger logger = LoggerFactory.getLogger(FcmSender.class);

  private final AccountsManager accountsManager;
  private final ExecutorService executor;
  private final FirebaseMessaging firebaseMessagingClient;

  private static final String SENT_MESSAGE_COUNTER_NAME = name(FcmSender.class, "sentMessage");

  public FcmSender(ExecutorService executor, AccountsManager accountsManager, String credentials) throws IOException {
    try (final ByteArrayInputStream credentialInputStream = new ByteArrayInputStream(credentials.getBytes(StandardCharsets.UTF_8))) {
      FirebaseOptions options = FirebaseOptions.builder()
          .setCredentials(GoogleCredentials.fromStream(credentialInputStream))
          .build();

      FirebaseApp.initializeApp(options);
    }

    this.executor = executor;
    this.accountsManager = accountsManager;
    this.firebaseMessagingClient = FirebaseMessaging.getInstance();
  }

  @VisibleForTesting
  public FcmSender(ExecutorService executor, AccountsManager accountsManager, FirebaseMessaging firebaseMessagingClient) {
    this.accountsManager = accountsManager;
    this.executor = executor;
    this.firebaseMessagingClient = firebaseMessagingClient;
  }

  public void sendMessage(GcmMessage message) {
    Message.Builder builder = Message.builder()
        .setToken(message.getGcmId())
        .setAndroidConfig(AndroidConfig.builder()
            .setPriority(AndroidConfig.Priority.HIGH)
            .build());

    final String key = switch (message.getType()) {
      case NOTIFICATION -> "notification";
      case CHALLENGE -> "challenge";
      case RATE_LIMIT_CHALLENGE -> "rateLimitChallenge";
    };

    builder.putData(key, message.getData().orElse(""));

    final ApiFuture<String> sendFuture = firebaseMessagingClient.sendAsync(builder.build());

    sendFuture.addListener(() -> {
      Tags tags = Tags.of("type", key);

      try {
        sendFuture.get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof final FirebaseMessagingException firebaseMessagingException) {
          tags = tags.and("errorCode", firebaseMessagingException.getMessagingErrorCode().name().toLowerCase());

          switch (firebaseMessagingException.getMessagingErrorCode()) {

            case UNREGISTERED -> handleBadRegistration(message);
            case THIRD_PARTY_AUTH_ERROR, INVALID_ARGUMENT, INTERNAL, QUOTA_EXCEEDED, SENDER_ID_MISMATCH, UNAVAILABLE ->
                logger.debug("Unrecoverable Error ::: (error={}}), (gcm_id={}}), (destination={}}), (device_id={}})",
                    firebaseMessagingException.getMessagingErrorCode(), message.getGcmId(), message.getUuid(), message.getDeviceId());
          }
        } else {
          throw new RuntimeException("Failed to send message", e);
        }
      } catch (InterruptedException e) {
        // This should never happen; by definition, if we're in the future's listener, the future is done, and so
        // `get()` should return immediately.
        throw new IllegalStateException("Interrupted while getting send future result", e);
      } finally {
        Metrics.counter(SENT_MESSAGE_COUNTER_NAME, tags).increment();
      }
    }, executor);
  }

  private void handleBadRegistration(GcmMessage message) {
    Optional<Account> account = getAccountForEvent(message);

    if (account.isPresent()) {
      //noinspection OptionalGetWithoutIsPresent
      Device device = account.get().getDevice(message.getDeviceId()).get();

      if (device.getUninstalledFeedbackTimestamp() == 0) {
        accountsManager.updateDevice(account.get(), message.getDeviceId(), d ->
            d.setUninstalledFeedbackTimestamp(Util.todayInMillis()));
      }
    }
  }

  private Optional<Account> getAccountForEvent(GcmMessage message) {
    Optional<Account> account = message.getUuid().flatMap(accountsManager::getByAccountIdentifier);

    if (account.isPresent()) {
      Optional<Device> device = account.get().getDevice(message.getDeviceId());

      if (device.isPresent()) {
        if (message.getGcmId().equals(device.get().getGcmId())) {

          if (device.get().getPushTimestamp() == 0 || System.currentTimeMillis() > (device.get().getPushTimestamp() + TimeUnit.SECONDS.toMillis(10))) {
            return account;
          }
        }
      }
    }

    return Optional.empty();
  }
}
