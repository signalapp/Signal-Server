/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

public class AnalyzeDeviceCapabilitiesCommand extends AbstractSinglePassCrawlAccountsCommand {

  private static final Duration MAX_DEVICE_IDLE_DURATION = Duration.ofDays(90);

  private static final Logger logger = LoggerFactory.getLogger(AnalyzeDeviceCapabilitiesCommand.class);

  public AnalyzeDeviceCapabilitiesCommand() {
    super("analyze-device-capabilities", "Analyze capability adoption among active devices");
  }

  @Override
  protected void crawlAccounts(final ParallelFlux<Account> accounts) {
    final AtomicLong senderKey = new AtomicLong(0);
    final AtomicLong announcementGroup = new AtomicLong(0);
    final AtomicLong changeNumber = new AtomicLong(0);
    final AtomicLong pni = new AtomicLong(0);
    final AtomicLong stories = new AtomicLong(0);
    final AtomicLong giftBadges = new AtomicLong(0);
    final AtomicLong paymentActivation = new AtomicLong(0);

    final long devicesAnalyzed = accounts
        .flatMap(account -> Flux.fromIterable(account.getDevices()))
        .filter(device -> {
          final Instant lastSeen = Instant.ofEpochMilli(device.getLastSeen());
          final Duration durationSinceLastSeen = Duration.between(lastSeen, Instant.now());

          return device.isEnabled() && durationSinceLastSeen.compareTo(MAX_DEVICE_IDLE_DURATION) <= 0;
        })
        .sequential()
        .mapNotNull(Device::getCapabilities)
        .doOnNext(deviceCapabilities -> {
          if (deviceCapabilities.isSenderKey()) {
            senderKey.incrementAndGet();
          }

          if (deviceCapabilities.isAnnouncementGroup()) {
            announcementGroup.incrementAndGet();
          }

          if (deviceCapabilities.isChangeNumber()) {
            changeNumber.incrementAndGet();
          }

          if (deviceCapabilities.isPni()) {
            pni.incrementAndGet();
          }

          if (deviceCapabilities.isStories()) {
            stories.incrementAndGet();
          }

          if (deviceCapabilities.isGiftBadges()) {
            giftBadges.incrementAndGet();
          }

          if (deviceCapabilities.isPaymentActivation()) {
            paymentActivation.incrementAndGet();
          }
        })
        .count()
        .blockOptional()
        .orElse(0L);

    logger.info("""
            Analyzed devices:   {}
            ----
            Sender key:         {} ({}%)
            Announcement group: {} ({}%)
            Change number:      {} ({}%)
            PNI:                {} ({}%)
            Stories:            {} ({}%)
            Gift badges:        {} ({}%)
            Payment activation: {} ({}%)
            """,
        devicesAnalyzed,
        senderKey.get(), 100 * ((double) senderKey.get()) / ((double) devicesAnalyzed),
        announcementGroup.get(), 100 * ((double) announcementGroup.get()) / ((double) devicesAnalyzed),
        changeNumber.get(), 100 * ((double) changeNumber.get()) / ((double) devicesAnalyzed),
        pni.get(), 100 * ((double) pni.get()) / ((double) devicesAnalyzed),
        stories.get(), 100 * ((double) stories.get()) / ((double) devicesAnalyzed),
        giftBadges.get(), 100 * ((double) giftBadges.get()) / ((double) devicesAnalyzed),
        paymentActivation.get(), 100 * ((double) paymentActivation.get()) / ((double) devicesAnalyzed));
  }
}
