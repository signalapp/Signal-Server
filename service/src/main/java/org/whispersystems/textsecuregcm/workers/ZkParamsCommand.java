/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.core.cli.Command;
import io.dropwizard.core.setup.Bootstrap;
import java.util.Base64;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.signal.libsignal.zkgroup.GenericServerPublicParams;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.ServerPublicParams;
import org.signal.libsignal.zkgroup.ServerSecretParams;

public class ZkParamsCommand extends Command {

  public ZkParamsCommand() {
    super("zkparams", "Generates server zkparams");
  }

  @Override
  public void configure(Subparser subparser) {

  }

  private static String formatKey(byte[] contents) {
        return Base64.getEncoder().encodeToString(contents);
      }

  @Override
  public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
    ServerSecretParams serverSecretParams = ServerSecretParams.generate();
    ServerPublicParams serverPublicParams = serverSecretParams.getPublicParams();

    System.out.println("Public: " + formatKey(serverPublicParams.serialize()));
    System.out.println("Private: " + formatKey(serverSecretParams.serialize()));

    GenericServerSecretParams genericZkSecretParams = GenericServerSecretParams.generate();
    GenericServerPublicParams genericZkPublicParams = genericZkSecretParams.getPublicParams();

    System.out.println("zk Public: " + formatKey(genericZkPublicParams.serialize()));
    System.out.println("zk Private: " + formatKey(genericZkSecretParams.serialize()));
  }

}
