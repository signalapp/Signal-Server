/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECPrivateKey;
import org.whispersystems.textsecuregcm.entities.MessageProtos.SenderCertificate;
import org.whispersystems.textsecuregcm.entities.MessageProtos.ServerCertificate;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

public class CertificateGenerator {

  private final ECPrivateKey      privateKey;
  private final int               expiresDays;
  private final ServerCertificate serverCertificate;

  public CertificateGenerator(byte[] serverCertificate, ECPrivateKey privateKey, int expiresDays)
      throws InvalidProtocolBufferException
  {
    this.privateKey        = privateKey;
    this.expiresDays       = expiresDays;
    this.serverCertificate = ServerCertificate.parseFrom(serverCertificate);
  }

  public byte[] createFor(Account account, Device device, boolean includeE164) throws InvalidKeyException {
    SenderCertificate.Certificate.Builder builder = SenderCertificate.Certificate.newBuilder()
                                                                                 .setSenderDevice(Math.toIntExact(device.getId()))
                                                                                 .setExpires(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(expiresDays))
                                                                                 .setIdentityKey(ByteString.copyFrom(Base64.getDecoder().decode(account.getIdentityKey())))
                                                                                 .setSigner(serverCertificate)
                                                                                 .setSenderUuid(account.getUuid().toString());

    if (includeE164) {
      builder.setSender(account.getNumber());
    }

    byte[] certificate = builder.build().toByteArray();
    byte[] signature;
    try {
      signature = Curve.calculateSignature(privateKey, certificate);
    } catch (org.signal.libsignal.protocol.InvalidKeyException e) {
      throw new InvalidKeyException(e);
    }

    return SenderCertificate.newBuilder()
                            .setCertificate(ByteString.copyFrom(certificate))
                            .setSignature(ByteString.copyFrom(signature))
                            .build()
                            .toByteArray();
  }

}
