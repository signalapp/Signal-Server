/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.signal.libsignal.protocol.util.KeyHelper;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;

public class TestUser {

  private final int registrationId;

  @Nullable
  private final Integer pniRegistrationId;

  private final byte[] unidentifiedAccessKey;

  @Nullable
  private final String phoneNumber;

  private final String accountPassword;

  private final byte[] registrationPassword;

  private UUID aciUuid;

  @Nullable
  private UUID pniUuid;

  public static TestUser createNumberless(final String accountPassword, final byte[] accountRecoveryPassword) {
    final int registrationId = KeyHelper.generateRegistrationId(false);
    final byte[] unidentifiedAccessKey = Operations.randomBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    return new TestUser(
        registrationId,
        null,
        null,
        unidentifiedAccessKey,
        accountPassword,
        accountRecoveryPassword);
  }

  public static TestUser createNumberlessForRecovery(final String accountPassword, final byte[] accountRecoveryPassword) {
    // Recovering a numberless account requires PNI keys (though they are discarded by the server)
    final int registrationId = KeyHelper.generateRegistrationId(false);
    final int pniRegistrationId = KeyHelper.generateRegistrationId(false);
    final byte[] unidentifiedAccessKey = new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH];
    new SecureRandom().nextBytes(unidentifiedAccessKey);

    return new TestUser(
        registrationId,
        pniRegistrationId,
        null,
        unidentifiedAccessKey,
        accountPassword,
        accountRecoveryPassword);
  }

  public static TestUser create(final String phoneNumber, final String accountPassword, final byte[] registrationPassword) {
    final int registrationId = KeyHelper.generateRegistrationId(false);
    final int pniRegistrationId = KeyHelper.generateRegistrationId(false);
    final byte[] unidentifiedAccessKey = new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH];
    new SecureRandom().nextBytes(unidentifiedAccessKey);

    return new TestUser(
        registrationId,
        pniRegistrationId,
        phoneNumber,
        unidentifiedAccessKey,
        accountPassword,
        registrationPassword);
  }

  public TestUser(
      final int registrationId,
      @Nullable final Integer pniRegistrationId,
      @Nullable final String phoneNumber,
      final byte[] unidentifiedAccessKey,
      final String accountPassword,
      final byte[] registrationPassword) {
    this.registrationId = registrationId;
    this.pniRegistrationId = pniRegistrationId;
    this.phoneNumber = phoneNumber;
    this.unidentifiedAccessKey = unidentifiedAccessKey;
    this.accountPassword = accountPassword;
    this.registrationPassword = registrationPassword;
  }

  public int registrationId() {
    return registrationId;
  }

  public Optional<String> phoneNumber() {
    return Optional.ofNullable(phoneNumber);
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

  public Optional<UUID> pniUuid() {
    return Optional.ofNullable(pniUuid);
  }

  public AccountAttributes accountAttributes() {
    return new AccountAttributes(true, registrationId, pniRegistrationId, "".getBytes(StandardCharsets.UTF_8), "", true,
        DeviceCapability.CAPABILITIES_REQUIRED_FOR_NEW_DEVICES, registrationPassword)
        .setUnidentifiedAccessKey(unidentifiedAccessKey);
  }

  public void setAciUuid(final UUID aciUuid) {
    this.aciUuid = aciUuid;
  }

  public void setPniUuid(@Nullable final UUID pniUuid) {
    this.pniUuid = pniUuid;
  }
}
