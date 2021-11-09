/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.google.common.base.MoreObjects;
import com.google.protobuf.ByteString;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPrivateKey;
import org.whispersystems.textsecuregcm.entities.MessageProtos;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.util.Base64;
import java.util.Set;

import io.dropwizard.cli.Command;
import io.dropwizard.setup.Bootstrap;

public class CertificateCommand extends Command {

  private static final Set<Integer> RESERVED_CERTIFICATE_IDS = Set.of(
      0xdeadc357 // Reserved for testing; see https://github.com/signalapp/libsignal-client/pull/118
  );

  public CertificateCommand() {
    super("certificate", "Generates server certificates for unidentified delivery");
  }

  @Override
  public void configure(Subparser subparser) {
    subparser.addArgument("-ca", "--ca")
             .dest("ca")
             .action(Arguments.storeTrue())
             .setDefault(Boolean.FALSE)
             .help("Generate CA parameters");

    subparser.addArgument("-k", "--key")
             .dest("key")
             .type(String.class)
             .help("The CA private signing key");

    subparser.addArgument("-i", "--id")
             .dest("keyId")
             .type(Integer.class)
             .help("The key ID to create");
  }

  @Override
  public void run(Bootstrap<?> bootstrap, Namespace namespace) throws Exception {
    if (MoreObjects.firstNonNull(namespace.getBoolean("ca"), false)) runCaCommand();
    else                                                                  runCertificateCommand(namespace);
  }

  private void runCaCommand() {
    ECKeyPair keyPair = Curve.generateKeyPair();
    System.out.println("Public key : " + Base64.getEncoder().encodeToString(keyPair.getPublicKey().serialize()));
    System.out.println("Private key: " + Base64.getEncoder().encodeToString(keyPair.getPrivateKey().serialize()));
  }

  private void runCertificateCommand(Namespace namespace) throws IOException, InvalidKeyException {
    if (namespace.getString("key") == null) {
      System.out.println("No key specified!");
      return;
    }

    if (namespace.getInt("keyId") == null) {
      System.out.print("No key id specified!");
      return;
    }

    ECPrivateKey key   = Curve.decodePrivatePoint(Base64.getDecoder().decode(namespace.getString("key")));
    int          keyId = namespace.getInt("keyId");

    if (RESERVED_CERTIFICATE_IDS.contains(keyId)) {
      throw new IllegalArgumentException(
          String.format("Key ID %08x has been reserved or revoked and may not be used in new certificates.", keyId));
    }

    ECKeyPair keyPair = Curve.generateKeyPair();

    byte[] certificate = MessageProtos.ServerCertificate.Certificate.newBuilder()
                                                                    .setId(keyId)
                                                                    .setKey(ByteString.copyFrom(keyPair.getPublicKey().serialize()))
                                                                    .build()
                                                                    .toByteArray();

    byte[] signature;
    try {
      signature = Curve.calculateSignature(key, certificate);
    } catch (org.signal.libsignal.protocol.InvalidKeyException e) {
      throw new InvalidKeyException(e);
    }

    byte[] signedCertificate = MessageProtos.ServerCertificate.newBuilder()
                                                              .setCertificate(ByteString.copyFrom(certificate))
                                                              .setSignature(ByteString.copyFrom(signature))
                                                              .build()
                                                              .toByteArray();

    System.out.println("Certificate: " + Base64.getEncoder().encodeToString(signedCertificate));
    System.out.println("Private key: " + Base64.getEncoder().encodeToString(keyPair.getPrivateKey().serialize()));
  }
}
