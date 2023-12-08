/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ProcessPushNotificationFeedbackCommand extends AbstractSinglePassCrawlAccountsCommand {

  private final Clock clock;

  @VisibleForTesting
  static final Duration MAX_TOKEN_REFRESH_DELAY = Duration.ofDays(3);

  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";

  private static final int MAX_CONCURRENCY = 16;

  private static final String EXPIRED_DEVICE_COUNTER_NAME =
      MetricsUtil.name(ProcessPushNotificationFeedbackCommand.class, "expiredDevice");

  private static final String RECOVERED_DEVICE_COUNTER_NAME =
      MetricsUtil.name(ProcessPushNotificationFeedbackCommand.class, "recoveredDevice");

  private static final Logger log = LoggerFactory.getLogger(ProcessPushNotificationFeedbackCommand.class);

  public ProcessPushNotificationFeedbackCommand(final Clock clock) {
    super("process-push-notification-feedback", "");

    this.clock = clock;
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don't actually modify accounts with stale devices");
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final boolean isDryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);

    accounts
        .filter(account -> account.getDevices().stream().anyMatch(this::deviceNeedsUpdate))
        .flatMap(account -> {
          account.getDevices().stream()
              .filter(this::deviceNeedsUpdate)
              .forEach(device -> {
                final Tags tags = Tags.of(
                    UserAgentTagUtil.PLATFORM_TAG, getPlatform(device),
                    "dryRun", String.valueOf(isDryRun));

                if (deviceExpired(device)) {
                  Metrics.counter(EXPIRED_DEVICE_COUNTER_NAME, tags).increment();
                } else {
                  Metrics.counter(RECOVERED_DEVICE_COUNTER_NAME, tags).increment();
                }
              });

          if (isDryRun) {
            return Mono.just(account);
          } else {
            return Mono.fromFuture(() -> getCommandDependencies().accountsManager().updateAsync(account,
                    a -> a.getDevices().stream()
                        .filter(this::deviceNeedsUpdate)
                        .forEach(device -> {
                          if (deviceExpired(device)) {
                            getUserAgent(device).ifPresent(device::setUserAgent);

                            device.setGcmId(null);
                            device.setApnId(null);
                            device.setVoipApnId(null);
                            device.setFetchesMessages(false);
                          } else {
                            device.setUninstalledFeedbackTimestamp(0);
                          }
                        })))
                .onErrorResume(throwable -> {
                  log.warn("Failed to process push notification feedback for account {}", account.getUuid(), throwable);
                  return Mono.empty();
                });
          }
        }, MAX_CONCURRENCY)
        .then()
        .block();
  }

  @VisibleForTesting
  boolean pushFeedbackIntervalElapsed(final Device device) {
    // After we get an indication that a device may have uninstalled the Signal app (`uninstalledFeedbackTimestamp` is
    // non-zero), check back in after a few days to see what ultimately happened.
    return device.getUninstalledFeedbackTimestamp() != 0 &&
        Instant.ofEpochMilli(device.getUninstalledFeedbackTimestamp()).plus(MAX_TOKEN_REFRESH_DELAY)
            .isBefore(clock.instant());
  }

  @VisibleForTesting
  boolean deviceExpired(final Device device) {
    // If we received an indication that a device may have uninstalled the Signal app and we haven't seen that device in
    // a few days since that event, we consider the device "expired" and should clean up its push tokens. If we have
    // seen the device recently, though, we assume that the "uninstalled" hint was either incorrect or the device has
    // since reinstalled the app and provided new push tokens.
    return Instant.ofEpochMilli(device.getLastSeen()).plus(MAX_TOKEN_REFRESH_DELAY).isBefore(clock.instant());
  }

  @VisibleForTesting
  boolean deviceNeedsUpdate(final Device device) {
    return pushFeedbackIntervalElapsed(device) && (device.isEnabled() || device.getLastSeen() > device.getUninstalledFeedbackTimestamp());
  }

  @VisibleForTesting
  static Optional<String> getUserAgent(final Device device) {
    if (StringUtils.isNotBlank(device.getApnId())) {
      if (device.isPrimary()) {
        return Optional.of("OWI");
      } else {
        return Optional.of("OWP");
      }
    } else if (StringUtils.isNotBlank(device.getGcmId())) {
      return Optional.of("OWA");
    }

    return Optional.empty();
  }

  private static String getPlatform(final Device device) {
    if (StringUtils.isNotBlank(device.getApnId())) {
      return "ios";
    } else if (StringUtils.isNotBlank(device.getGcmId())) {
      return "android";
    }

    return "unrecognized";
  }
}
