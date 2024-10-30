/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.security.SecureRandom;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.IdentityKeyPair;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.protocol.state.SignedPreKeyRecord;
import org.signal.libsignal.protocol.util.KeyHelper;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.storage.Device;

public class TestUser {

  private final int registrationId;

  private final int pniRegistrationId;

  private final IdentityKeyPair aciIdentityKey;

  private final Map<Byte, TestDevice> devices = new ConcurrentHashMap<>();

  private final byte[] unidentifiedAccessKey;

  private String phoneNumber;

  private IdentityKeyPair pniIdentityKey;

  private String accountPassword;

  private byte[] registrationPassword;

  private UUID aciUuid;

  private UUID pniUuid;


  public static TestUser create(final String phoneNumber, final String accountPassword, final byte[] registrationPassword) {
    // ACI identity key pair
    final IdentityKeyPair aciIdentityKey = IdentityKeyPair.generate();
    // PNI identity key pair
    final IdentityKeyPair pniIdentityKey = IdentityKeyPair.generate();
    // registration id
    final int registrationId = KeyHelper.generateRegistrationId(false);
    final int pniRegistrationId = KeyHelper.generateRegistrationId(false);
    // uak
    final byte[] unidentifiedAccessKey = new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH];
    new SecureRandom().nextBytes(unidentifiedAccessKey);

    return new TestUser(
        registrationId,
        pniRegistrationId,
        aciIdentityKey,
        phoneNumber,
        pniIdentityKey,
        unidentifiedAccessKey,
        accountPassword,
        registrationPassword);
  }

  public TestUser(
      final int registrationId,
      final int pniRegistrationId,
      final IdentityKeyPair aciIdentityKey,
      final String phoneNumber,
      final IdentityKeyPair pniIdentityKey,
      final byte[] unidentifiedAccessKey,
      final String accountPassword,
      final byte[] registrationPassword) {
    this.registrationId = registrationId;
    this.pniRegistrationId = pniRegistrationId;
    this.aciIdentityKey = aciIdentityKey;
    this.phoneNumber = phoneNumber;
    this.pniIdentityKey = pniIdentityKey;
    this.unidentifiedAccessKey = unidentifiedAccessKey;
    this.accountPassword = accountPassword;
    this.registrationPassword = registrationPassword;
    devices.put(Device.PRIMARY_ID, TestDevice.create(Device.PRIMARY_ID, aciIdentityKey, pniIdentityKey));
  }

  public int registrationId() {
    return registrationId;
  }

  public IdentityKeyPair aciIdentityKey() {
    return aciIdentityKey;
  }

  public String phoneNumber() {
    return phoneNumber;
  }

  public IdentityKeyPair pniIdentityKey() {
    return pniIdentityKey;
  }

  public String accountPassword() {
    return accountPassword;
  }

  public byte[] registrationPassword() {
    return registrationPassword;
  }

  public UUID aciUuid() {
    return aciUuid;
  }

  public UUID pniUuid() {
    return pniUuid;
  }

  public AccountAttributes accountAttributes() {
    return new AccountAttributes(true, registrationId, pniRegistrationId, "".getBytes(StandardCharsets.UTF_8), "", true, Set.of())
        .withUnidentifiedAccessKey(unidentifiedAccessKey)
        .withRecoveryPassword(registrationPassword);
  }

  public void setAciUuid(final UUID aciUuid) {
    this.aciUuid = aciUuid;
  }

  public void setPniUuid(final UUID pniUuid) {
    this.pniUuid = pniUuid;
  }

  public void setPhoneNumber(final String phoneNumber) {
    this.phoneNumber = phoneNumber;
  }

  public void setPniIdentityKey(final IdentityKeyPair pniIdentityKey) {
    this.pniIdentityKey = pniIdentityKey;
  }

  public void setAccountPassword(final String accountPassword) {
    this.accountPassword = accountPassword;
  }

  public void setRegistrationPassword(final byte[] registrationPassword) {
    this.registrationPassword = registrationPassword;
  }

  public PreKeySetPublicView preKeys(final byte deviceId, final boolean pni) {
    final IdentityKeyPair identity = pni
        ? pniIdentityKey
        : aciIdentityKey;
    final TestDevice device = requireNonNull(devices.get(deviceId));
    final SignedPreKeyRecord signedPreKeyRecord = device.latestSignedPreKey(identity);
    try {
      return new PreKeySetPublicView(
          Collections.emptyList(),
          identity.getPublicKey(),
          new SignedPreKeyPublicView(
              signedPreKeyRecord.getId(),
              signedPreKeyRecord.getKeyPair().getPublicKey(),
              signedPreKeyRecord.getSignature()
          )
      );
    } catch (InvalidKeyException e) {
      throw new RuntimeException(e);
    }
  }

  public record SignedPreKeyPublicView(
      int keyId,
      @JsonSerialize(using = Codecs.ECPublicKeySerializer.class)
      @JsonDeserialize(using = Codecs.ECPublicKeyDeserializer.class)
      ECPublicKey publicKey,
      @JsonSerialize(using = Codecs.ByteArraySerializer.class)
      @JsonDeserialize(using = Codecs.ByteArrayDeserializer.class)
      byte[] signature) {
  }

  public record PreKeySetPublicView(
      List<String> preKeys,
      @JsonSerialize(using = Codecs.IdentityKeySerializer.class)
      @JsonDeserialize(using = Codecs.IdentityKeyDeserializer.class)
      IdentityKey identityKey,
      SignedPreKeyPublicView signedPreKey) {
  }
}
