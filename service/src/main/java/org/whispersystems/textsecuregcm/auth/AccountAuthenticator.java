/*
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
package org.whispersystems.textsecuregcm.auth;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import org.apache.http.auth.AUTH;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

import java.util.List;
import java.util.Optional;

import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicCredentials;

import static com.codahale.metrics.MetricRegistry.name;

public class AccountAuthenticator extends BaseAccountAuthenticator implements Authenticator<BasicCredentials, Account> {

  private static final String AUTHENTICATION_COUNTER_NAME     = name(AccountAuthenticator.class, "authenticate");
  private static final String GV2_CAPABLE_TAG_NAME            = "gv2";
  private static final String IOS_OLD_GV2_CAPABILITY_TAG_NAME = "ios_old_gv2";

  public AccountAuthenticator(AccountsManager accountsManager) {
    super(accountsManager);
  }

  @Override
  public Optional<Account> authenticate(BasicCredentials basicCredentials) {
    final Optional<Account> maybeAccount = super.authenticate(basicCredentials, true);

    // TODO Remove this temporary counter after the GV2 rollout is underway
    maybeAccount.ifPresent(account -> {
      final boolean iosDeviceHasOldGv2Capability = account.getDevices().stream().anyMatch(device ->
              (device.getApnId() != null || device.getVoipApnId() != null) &&
              (device.getCapabilities() != null && device.getCapabilities().isGv2()));

      final Tag gv2CapableTag = Tag.of(GV2_CAPABLE_TAG_NAME, String.valueOf(account.isGroupsV2Supported()));
      final Tag iosOldGv2CapabilityTag = Tag.of(IOS_OLD_GV2_CAPABILITY_TAG_NAME, String.valueOf(iosDeviceHasOldGv2Capability));

      Metrics.counter(AUTHENTICATION_COUNTER_NAME, List.of(gv2CapableTag, iosOldGv2CapabilityTag)).increment();
    });

    return maybeAccount;
  }

}
