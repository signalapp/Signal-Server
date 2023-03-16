/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.fasterxml.jackson.databind.DeserializationFeature;
import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.setup.Environment;
import java.util.UUID;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

public class UnlinkDeviceCommand extends EnvironmentCommand<WhisperServerConfiguration> {

  public UnlinkDeviceCommand() {
    super(new Application<>() {
      @Override
      public void run(WhisperServerConfiguration configuration, Environment environment) {

      }
    }, "unlink-device", "Unlink a device and clear messages");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("-d", "--deviceId")
        .dest("deviceId")
        .type(Long.class)
        .required(true);

    subparser.addArgument("-u", "--uuid")
        .help("the UUID of the account to modify")
        .dest("uuid")
        .type(String.class)
        .required(true);
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration) throws Exception {
    environment.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    final UUID aci = UUID.fromString(namespace.getString("uuid").trim());
    final long deviceId = namespace.getLong("deviceId");

    final CommandDependencies deps = CommandDependencies.build("unlink-device", environment, configuration);

    Account account = deps.accountsManager().getByAccountIdentifier(aci)
        .orElseThrow(() -> new IllegalArgumentException("account id " + aci +" does not exist"));

    if (deviceId == Device.MASTER_ID) {
      throw new IllegalArgumentException("cannot delete primary device");
    }

    /** see {@link org.whispersystems.textsecuregcm.controllers.DeviceController#removeDevice} */
    System.out.format("Removing device %s::%d\n", aci, deviceId);
    account = deps.accountsManager().update(account, a -> a.removeDevice(deviceId));

    System.out.format("Removing keys for device %s::%d\n", aci, deviceId);
    deps.keys().delete(account.getUuid(), deviceId);

    System.out.format("Clearing additional messages for %s::%d\n", aci, deviceId);
    deps.messagesManager().clear(account.getUuid(), deviceId);

    System.out.format("Clearing presence state for %s::%d\n", aci, deviceId);
    deps.clientPresenceManager().disconnectPresence(aci, deviceId);

    System.out.format("Device %s::%d successfully removed\n", aci, deviceId);


  }
}
