package org.whispersystems.textsecuregcm.push;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.UnregisteredEvent;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.dropwizard.lifecycle.Managed;

public class FeedbackHandler implements Managed, Runnable {

  private final Logger logger = LoggerFactory.getLogger(PushServiceClient.class);

  private final PushServiceClient client;
  private final AccountsManager   accountsManager;

  private ScheduledExecutorService executor;

  public FeedbackHandler(PushServiceClient client, AccountsManager accountsManager) {
    this.client          = client;
    this.accountsManager = accountsManager;
  }

  @Override
  public void start() throws Exception {
    this.executor = Executors.newSingleThreadScheduledExecutor();
    this.executor.scheduleAtFixedRate(this, 0, 1, TimeUnit.MINUTES);
  }

  @Override
  public void stop() throws Exception {
    if (this.executor != null) {
      this.executor.shutdown();
    }
  }

  @Override
  public void run() {
    try {
      List<UnregisteredEvent> gcmFeedback = client.getGcmFeedback();
      List<UnregisteredEvent> apnFeedback = client.getApnFeedback();

      for (UnregisteredEvent gcmEvent : gcmFeedback) {
        handleGcmUnregistered(gcmEvent);
      }

      for (UnregisteredEvent apnEvent : apnFeedback) {
        handleApnUnregistered(apnEvent);
      }
    } catch (IOException e) {
      logger.warn("Error retrieving feedback: ", e);
    }
  }

  private void handleGcmUnregistered(UnregisteredEvent event) {
    logger.info("Got GCM Unregistered: " + event.getNumber() + "," + event.getDeviceId());

    Optional<Account> account = accountsManager.get(event.getNumber());

    if (account.isPresent()) {
      Optional<Device> device = account.get().getDevice(event.getDeviceId());

      if (device.isPresent()) {
        if (event.getRegistrationId().equals(device.get().getGcmId())) {
          logger.info("GCM Unregister GCM ID matches!");
          if (device.get().getPushTimestamp() == 0 ||
              event.getTimestamp() > device.get().getPushTimestamp())
          {
            logger.info("GCM Unregister Timestamp matches!");

            if (event.getCanonicalId() != null && !event.getCanonicalId().isEmpty()) {
              logger.info("It's a canonical ID update...");
              device.get().setGcmId(event.getCanonicalId());
            } else {
              device.get().setGcmId(null);
            }
            accountsManager.update(account.get());
          }
        }
      }
    }
  }

  private void handleApnUnregistered(UnregisteredEvent event) {
    logger.info("Got APN Unregistered: " + event.getNumber() + "," + event.getDeviceId());

    Optional<Account> account = accountsManager.get(event.getNumber());

    if (account.isPresent()) {
      Optional<Device> device = account.get().getDevice(event.getDeviceId());

      if (device.isPresent()) {
        if (event.getRegistrationId().equals(device.get().getApnId())) {
          logger.info("APN Unregister APN ID matches!");
          if (device.get().getPushTimestamp() == 0 ||
              event.getTimestamp() > device.get().getPushTimestamp())
          {
            logger.info("APN Unregister timestamp matches!");
            device.get().setApnId(null);
            accountsManager.update(account.get());
          }
        }
      }
    }
  }
}
