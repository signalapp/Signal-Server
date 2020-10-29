/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.auth.basic.BasicCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Util;

import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import static com.codahale.metrics.MetricRegistry.name;

public class BaseAccountAuthenticator {

  private final MetricRegistry metricRegistry               = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          authenticationFailedMeter    = metricRegistry.meter(name(getClass(), "authentication", "failed"         ));
  private final Meter          authenticationSucceededMeter = metricRegistry.meter(name(getClass(), "authentication", "succeeded"      ));
  private final Meter          noSuchAccountMeter           = metricRegistry.meter(name(getClass(), "authentication", "noSuchAccount"  ));
  private final Meter          noSuchDeviceMeter            = metricRegistry.meter(name(getClass(), "authentication", "noSuchDevice"   ));
  private final Meter          accountDisabledMeter         = metricRegistry.meter(name(getClass(), "authentication", "accountDisabled"));
  private final Meter          deviceDisabledMeter          = metricRegistry.meter(name(getClass(), "authentication", "deviceDisabled" ));
  private final Meter          invalidAuthHeaderMeter       = metricRegistry.meter(name(getClass(), "authentication", "invalidHeader"  ));

  private final Logger logger = LoggerFactory.getLogger(AccountAuthenticator.class);

  private final AccountsManager accountsManager;
  private final Clock           clock;

  public BaseAccountAuthenticator(AccountsManager accountsManager) {
    this(accountsManager, Clock.systemUTC());
  }

  @VisibleForTesting
  public BaseAccountAuthenticator(AccountsManager accountsManager, Clock clock) {
    this.accountsManager = accountsManager;
    this.clock           = clock;
  }

  public Optional<Account> authenticate(BasicCredentials basicCredentials, boolean enabledRequired) {
    try {
      AuthorizationHeader authorizationHeader = AuthorizationHeader.fromUserAndPassword(basicCredentials.getUsername(), basicCredentials.getPassword());
      Optional<Account>   account             = accountsManager.get(authorizationHeader.getIdentifier());

      if (!account.isPresent()) {
        noSuchAccountMeter.mark();
        return Optional.empty();
      }

      Optional<Device> device = account.get().getDevice(authorizationHeader.getDeviceId());

      if (!device.isPresent()) {
        noSuchDeviceMeter.mark();
        return Optional.empty();
      }

      if (enabledRequired) {
        if (!device.get().isEnabled()) {
          deviceDisabledMeter.mark();
          return Optional.empty();
        }

        if (!account.get().isEnabled()) {
          accountDisabledMeter.mark();
          return Optional.empty();
        }
      }

      if (device.get().getAuthenticationCredentials().verify(basicCredentials.getPassword())) {
        authenticationSucceededMeter.mark();
        account.get().setAuthenticatedDevice(device.get());
        updateLastSeen(account.get(), device.get());
        return account;
      }

      authenticationFailedMeter.mark();
      return Optional.empty();
    } catch (IllegalArgumentException | InvalidAuthorizationHeaderException iae) {
      invalidAuthHeaderMeter.mark();
      return Optional.empty();
    }
  }

  @VisibleForTesting
  public void updateLastSeen(Account account, Device device) {
    final long lastSeenOffsetSeconds   = Math.abs(account.getUuid().getLeastSignificantBits()) % ChronoUnit.DAYS.getDuration().toSeconds();
    final long todayInMillisWithOffset = Util.todayInMillisGivenOffsetFromNow(clock, Duration.ofSeconds(lastSeenOffsetSeconds).negated());

    if (device.getLastSeen() < todayInMillisWithOffset) {
      device.setLastSeen(Util.todayInMillis(clock));
      accountsManager.update(account);
    }
  }

}
