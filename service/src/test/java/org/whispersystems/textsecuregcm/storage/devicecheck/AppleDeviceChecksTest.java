/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage.devicecheck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.webauthn4j.appattest.DeviceCheckManager;
import com.webauthn4j.appattest.authenticator.DCAppleDevice;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;


class AppleDeviceChecksTest {

  private static final UUID ACI = UUID.randomUUID();

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.APPLE_DEVICE_CHECKS,
      DynamoDbExtensionSchema.Tables.APPLE_DEVICE_CHECKS_KEY_CONSTRAINT);

  private AppleDeviceChecks deviceChecks;
  private Account account;

  @BeforeEach
  void setupDeviceChecks() {
    account = mock(Account.class);
    when(account.getUuid()).thenReturn(ACI);
    deviceChecks = new AppleDeviceChecks(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DeviceCheckManager.createObjectConverter(),
        DynamoDbExtensionSchema.Tables.APPLE_DEVICE_CHECKS.tableName(),
        DynamoDbExtensionSchema.Tables.APPLE_DEVICE_CHECKS_KEY_CONSTRAINT.tableName());
  }

  @Test
  public void testSerde() throws DuplicatePublicKeyException {
    final DCAppleDevice appleDevice = DeviceCheckTestUtil.appleSampleDevice();
    final byte[] keyId = appleDevice.getAttestedCredentialData().getCredentialId();
    assertThat(deviceChecks.storeAttestation(account, keyId, appleDevice)).isTrue();

    assertThat(deviceChecks.keyIds(account)).containsExactly(keyId);

    final DCAppleDevice deserialized = deviceChecks.lookup(account, keyId).orElseThrow();
    assertThat(deserialized.getClass()).isEqualTo(appleDevice.getClass());
    assertThat(deserialized.getAttestationStatement().getFormat())
        .isEqualTo(appleDevice.getAttestationStatement().getFormat());
    assertThat(deserialized.getAttestationStatement().getClass())
        .isEqualTo(appleDevice.getAttestationStatement().getClass());
    assertThat(deserialized.getAttestedCredentialData().getCredentialId())
        .isEqualTo(appleDevice.getAttestedCredentialData().getCredentialId());
    assertThat(deserialized.getAttestedCredentialData().getCOSEKey())
        .isEqualTo(appleDevice.getAttestedCredentialData().getCOSEKey());
    assertThat(deserialized.getAttestedCredentialData().getAaguid())
        .isEqualTo(appleDevice.getAttestedCredentialData().getAaguid());
    assertThat(deserialized.getAuthenticatorExtensions().getExtensions())
        .containsExactlyEntriesOf(appleDevice.getAuthenticatorExtensions().getExtensions());
    assertThat(deserialized.getCounter())
        .isEqualTo(appleDevice.getCounter());
  }

  @Test
  public void duplicateKeys() throws DuplicatePublicKeyException {
    final DCAppleDevice appleDevice = DeviceCheckTestUtil.appleSampleDevice();
    final byte[] keyId = appleDevice.getAttestedCredentialData().getCredentialId();

    final Account dupliateAccount = mock(Account.class);
    when(dupliateAccount.getUuid()).thenReturn(UUID.randomUUID());

    deviceChecks.storeAttestation(account, keyId, appleDevice);

    // Storing same key with a different account fails
    assertThatExceptionOfType(DuplicatePublicKeyException.class)
        .isThrownBy(() -> deviceChecks.storeAttestation(dupliateAccount, keyId, appleDevice));

    // Storing the same key with the same account is fine
    assertThatNoException().isThrownBy(() -> deviceChecks.storeAttestation(account, keyId, appleDevice));
  }

  @Test
  public void multipleKeys() throws DuplicatePublicKeyException {
    final DCAppleDevice appleDevice = DeviceCheckTestUtil.appleSampleDevice();

    final List<byte[]> keyIds = IntStream.range(0, 10).mapToObj(i -> TestRandomUtil.nextBytes(16)).toList();

    for (byte[] keyId : keyIds) {
      // The keyId should typically match the device attestation, but we don't check that at this layer
      assertThat(deviceChecks.storeAttestation(account, keyId, appleDevice)).isTrue();
      assertThat(deviceChecks.lookup(account, keyId)).isNotEmpty();
    }
    final List<byte[]> actual = deviceChecks.keyIds(account);

    assertThat(actual).containsExactlyInAnyOrderElementsOf(keyIds);
  }

  @Test
  public void updateCounter() throws DuplicatePublicKeyException {
    final DCAppleDevice appleDevice = DeviceCheckTestUtil.appleSampleDevice();
    final byte[] keyId = appleDevice.getAttestedCredentialData().getCredentialId();

    assertThat(appleDevice.getCounter()).isEqualTo(0L);
    deviceChecks.storeAttestation(account, keyId, appleDevice);
    assertThat(deviceChecks.lookup(account, keyId).get().getCounter()).isEqualTo(0L);
    assertThat(deviceChecks.updateCounter(account, keyId, 2)).isTrue();
    assertThat(deviceChecks.lookup(account, keyId).get().getCounter()).isEqualTo(2L);

    // Should not update since the counter is stale
    assertThat(deviceChecks.updateCounter(account, keyId, 1)).isFalse();
    assertThat(deviceChecks.lookup(account, keyId).get().getCounter()).isEqualTo(2L);

  }
}
