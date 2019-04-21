package org.whispersystems.textsecuregcm.workers;

import com.google.common.base.MoreObjects;
import com.google.protobuf.ByteString;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.whispersystems.curve25519.Curve25519;
import org.whispersystems.curve25519.Curve25519KeyPair;
import org.whispersystems.textsecuregcm.crypto.Curve;
import org.whispersystems.textsecuregcm.crypto.ECKeyPair;
import org.whispersystems.textsecuregcm.crypto.ECPrivateKey;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.util.Base64;

import java.io.IOException;
import java.security.InvalidKeyException;

import io.dropwizard.cli.Command;
import io.dropwizard.setup.Bootstrap;

public class CertificateCommand extends Command {

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
    System.out.println("Public key : " + Base64.encodeBytes(keyPair.getPublicKey().serialize()));
    System.out.println("Private key: " + Base64.encodeBytes(keyPair.getPrivateKey().serialize()));
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

    ECPrivateKey key   = Curve.decodePrivatePoint(Base64.decode(namespace.getString("key")));
    int          keyId = namespace.getInt("keyId");

    ECKeyPair keyPair = Curve.generateKeyPair();

    byte[] certificate = MessageProtos.ServerCertificate.Certificate.newBuilder()
                                                                    .setId(keyId)
                                                                    .setKey(ByteString.copyFrom(keyPair.getPublicKey().serialize()))
                                                                    .build()
                                                                    .toByteArray();

    byte[] signature = Curve.calculateSignature(key, certificate);

    byte[] signedCertificate = MessageProtos.ServerCertificate.newBuilder()
                                                              .setCertificate(ByteString.copyFrom(certificate))
                                                              .setSignature(ByteString.copyFrom(signature))
                                                              .build()
                                                              .toByteArray();

    System.out.println("Certificate: " + Base64.encodeBytes(signedCertificate));
    System.out.println("Private key: " + Base64.encodeBytes(keyPair.getPrivateKey().serialize()));
  }
}
